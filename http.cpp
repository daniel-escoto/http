#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <queue>
#include <string>
#include <vector>

static const size_t BUFFER_SIZE = 8192;

static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_ready = PTHREAD_COND_INITIALIZER;
static std::queue<int> client_queue;

struct ThreadData {
        bool redundancy_enabled;
};

struct Request {
        std::string method;
        std::string path;
        std::vector<char> body;
};

static bool send_all(int fd, const char *data, size_t length) {
        while (length > 0) {
                ssize_t written = send(fd, data, length, 0);
                if (written <= 0) {
                        return false;
                }
                data += written;
                length -= (size_t)written;
        }
        return true;
}

static bool send_all(int fd, const std::string &data) {
        return send_all(fd, data.data(), data.size());
}

static void send_response(int fd, int status, const char *reason,
                          const std::string &body = "") {
        char header[256];
        int header_length = snprintf(header, sizeof(header),
                                     "HTTP/1.1 %d %s\r\nContent-Length: %zu\r\n\r\n",
                                     status, reason, body.size());
        if (header_length < 0 || (size_t)header_length >= sizeof(header)) {
                return;
        }
        send_all(fd, header, (size_t)header_length);
        if (!body.empty()) {
                send_all(fd, body);
        }
}

static bool is_valid_path(const std::string &path) {
        if (path.size() != 11 || path[0] != '/') {
                return false;
        }
        for (size_t i = 1; i < path.size(); ++i) {
                if (!isalnum((unsigned char)path[i])) {
                        return false;
                }
        }
        return true;
}

static std::string resource_name(const std::string &path) {
        if (!path.empty() && path[0] == '/') {
                return path.substr(1);
        }
        return path;
}

static bool starts_with(const std::string &prefix, const char *value) {
        return strncmp(value, prefix.c_str(), prefix.size()) == 0;
}

static bool ensure_directory(const std::string &path) {
        if (mkdir(path.c_str(), S_IRWXU) == 0) {
                return true;
        }
        return errno == EEXIST;
}

static bool write_file(const std::string &path, const std::vector<char> &body) {
        int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        if (fd == -1) {
                return false;
        }

        bool ok = true;
        size_t offset = 0;
        while (offset < body.size()) {
                ssize_t written = write(fd, body.data() + offset, body.size() - offset);
                if (written <= 0) {
                        ok = false;
                        break;
                }
                offset += (size_t)written;
        }

        close(fd);
        return ok;
}

static bool read_file(const std::string &path, std::vector<char> *body) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd == -1) {
                return false;
        }

        body->clear();
        char buffer[BUFFER_SIZE];
        ssize_t bytes_read;
        while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0) {
                body->insert(body->end(), buffer, buffer + bytes_read);
        }

        close(fd);
        return bytes_read == 0;
}

static bool copy_file(const std::string &dest, const std::string &src) {
        std::vector<char> body;
        return read_file(src, &body) && write_file(dest, body);
}

static bool send_file(int client_fd, const std::string &path) {
        std::vector<char> body;
        if (!read_file(path, &body)) {
                if (access(path.c_str(), F_OK) == 0) {
                        send_response(client_fd, 403, "Forbidden");
                } else {
                        send_response(client_fd, 404, "File Not Found");
                }
                return false;
        }

        char header[256];
        int header_length = snprintf(header, sizeof(header),
                                     "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n",
                                     body.size());
        if (header_length < 0 || (size_t)header_length >= sizeof(header)) {
                send_response(client_fd, 500, "Internal Server Error");
                return false;
        }

        send_all(client_fd, header, (size_t)header_length);
        if (!body.empty()) {
                send_all(client_fd, body.data(), body.size());
        }
        return true;
}

static bool is_regular_resource_name(const char *name) {
        std::string path = "/";
        path += name;
        return is_valid_path(path);
}

static void handle_backup(int client_fd) {
        char timestamp[32];
        snprintf(timestamp, sizeof(timestamp), "%ld", (long)time(NULL));

        std::string backup_dir = "backup-";
        backup_dir += timestamp;
        if (!ensure_directory(backup_dir)) {
                send_response(client_fd, 500, "Internal Server Error");
                return;
        }

        DIR *dir = opendir(".");
        if (dir == NULL) {
                send_response(client_fd, 500, "Internal Server Error");
                return;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
                if (is_regular_resource_name(entry->d_name)) {
                        std::string dest = backup_dir + "/" + entry->d_name;
                        copy_file(dest, entry->d_name);
                }
        }
        closedir(dir);

        send_response(client_fd, 200, "OK");
}

static bool restore_from_directory(const std::string &backup_dir) {
        DIR *dir = opendir(backup_dir.c_str());
        if (dir == NULL) {
                return false;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
                if (is_regular_resource_name(entry->d_name)) {
                        std::string src = backup_dir + "/" + entry->d_name;
                        copy_file(entry->d_name, src);
                }
        }
        closedir(dir);
        return true;
}

static std::string most_recent_backup() {
        DIR *dir = opendir(".");
        if (dir == NULL) {
                return "";
        }

        long newest = -1;
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
                if (starts_with("backup-", entry->d_name)) {
                        char *end = NULL;
                        long timestamp = strtol(entry->d_name + strlen("backup-"), &end, 10);
                        if (end != NULL && *end == '\0' && timestamp > newest) {
                                newest = timestamp;
                        }
                }
        }
        closedir(dir);

        if (newest < 0) {
                return "";
        }

        char path[64];
        snprintf(path, sizeof(path), "backup-%ld", newest);
        return path;
}

static void handle_restore(int client_fd, const std::string &path) {
        std::string backup_dir;
        if (path == "/r") {
                backup_dir = most_recent_backup();
                if (backup_dir.empty()) {
                        send_response(client_fd, 404, "File Not Found");
                        return;
                }
        } else if (path.rfind("/r/", 0) == 0) {
                backup_dir = "backup-" + path.substr(3);
        } else {
                send_response(client_fd, 400, "Bad Request");
                return;
        }

        if (!restore_from_directory(backup_dir)) {
                if (access(backup_dir.c_str(), F_OK) == 0) {
                        send_response(client_fd, 403, "Forbidden");
                } else {
                        send_response(client_fd, 404, "File Not Found");
                }
                return;
        }

        send_response(client_fd, 200, "OK");
}

static void handle_list_backups(int client_fd) {
        DIR *dir = opendir(".");
        if (dir == NULL) {
                send_response(client_fd, 500, "Internal Server Error");
                return;
        }

        std::vector<std::string> backups;
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
                if (starts_with("backup-", entry->d_name)) {
                        backups.push_back(entry->d_name + strlen("backup-"));
                }
        }
        closedir(dir);

        std::sort(backups.begin(), backups.end());

        std::string body;
        for (size_t i = 0; i < backups.size(); ++i) {
                body += backups[i];
                body += "\n";
        }
        send_response(client_fd, 200, "OK", body);
}

static bool redundancy_paths(const std::string &name, std::vector<std::string> *paths) {
        if (!ensure_directory("copy1") || !ensure_directory("copy2") ||
            !ensure_directory("copy3")) {
                return false;
        }

        paths->clear();
        paths->push_back("copy1/" + name);
        paths->push_back("copy2/" + name);
        paths->push_back("copy3/" + name);
        return true;
}

static void handle_put(int client_fd, const Request &request, bool redundancy_enabled) {
        if (!is_valid_path(request.path)) {
                send_response(client_fd, 400, "Bad Request");
                return;
        }

        std::string name = resource_name(request.path);
        if (redundancy_enabled) {
                std::vector<std::string> paths;
                if (!redundancy_paths(name, &paths)) {
                        send_response(client_fd, 500, "Internal Server Error");
                        return;
                }

                for (size_t i = 0; i < paths.size(); ++i) {
                        if (!write_file(paths[i], request.body)) {
                                send_response(client_fd, 500, "Internal Server Error");
                                return;
                        }
                }
        } else if (!write_file(name, request.body)) {
                send_response(client_fd, 500, "Internal Server Error");
                return;
        }

        send_response(client_fd, 200, "OK");
}

static void handle_redundant_get(int client_fd, const std::string &name) {
        std::vector<std::string> paths;
        if (!redundancy_paths(name, &paths)) {
                send_response(client_fd, 500, "Internal Server Error");
                return;
        }

        std::vector<char> copy1;
        std::vector<char> copy2;
        std::vector<char> copy3;
        if (!read_file(paths[0], &copy1) || !read_file(paths[1], &copy2) ||
            !read_file(paths[2], &copy3)) {
                send_response(client_fd, 404, "File Not Found");
                return;
        }

        if (copy1 == copy2 || copy1 == copy3) {
                send_response(client_fd, 200, "OK",
                              std::string(copy1.begin(), copy1.end()));
        } else if (copy2 == copy3) {
                send_response(client_fd, 200, "OK",
                              std::string(copy2.begin(), copy2.end()));
        } else {
                send_response(client_fd, 500, "Internal Server Error");
        }
}

static void handle_get(int client_fd, const Request &request, bool redundancy_enabled) {
        if (request.path == "/b") {
                handle_backup(client_fd);
                return;
        }
        if (request.path == "/r" || request.path.rfind("/r/", 0) == 0) {
                handle_restore(client_fd, request.path);
                return;
        }
        if (request.path == "/l") {
                handle_list_backups(client_fd);
                return;
        }
        if (!is_valid_path(request.path)) {
                send_response(client_fd, 400, "Bad Request");
                return;
        }

        std::string name = resource_name(request.path);
        if (redundancy_enabled) {
                handle_redundant_get(client_fd, name);
                return;
        }

        send_file(client_fd, name);
}

static bool parse_content_length(const std::string &headers, size_t *content_length) {
        std::string key = "Content-Length:";
        size_t pos = headers.find(key);
        if (pos == std::string::npos) {
                *content_length = 0;
                return true;
        }

        pos += key.size();
        while (pos < headers.size() && isspace((unsigned char)headers[pos])) {
                ++pos;
        }

        char *end = NULL;
        unsigned long parsed = strtoul(headers.c_str() + pos, &end, 10);
        if (end == headers.c_str() + pos) {
                return false;
        }

        *content_length = (size_t)parsed;
        return true;
}

static bool read_request(int client_fd, Request *request) {
        std::string raw;
        char buffer[BUFFER_SIZE];
        size_t header_end = std::string::npos;

        while (header_end == std::string::npos) {
                ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
                if (bytes_read <= 0) {
                        return false;
                }
                raw.append(buffer, (size_t)bytes_read);
                header_end = raw.find("\r\n\r\n");
                if (raw.size() > 65536) {
                        return false;
                }
        }

        std::string headers = raw.substr(0, header_end);
        size_t body_start = header_end + 4;

        size_t first_line_end = headers.find("\r\n");
        std::string request_line = headers.substr(0, first_line_end);
        size_t method_end = request_line.find(' ');
        size_t path_end = request_line.find(' ', method_end + 1);
        if (method_end == std::string::npos || path_end == std::string::npos) {
                return false;
        }

        request->method = request_line.substr(0, method_end);
        request->path = request_line.substr(method_end + 1, path_end - method_end - 1);

        size_t content_length = 0;
        if (!parse_content_length(headers, &content_length)) {
                return false;
        }

        if (raw.size() > body_start) {
                request->body.assign(raw.begin() + body_start, raw.end());
        } else {
                request->body.clear();
        }

        while (request->body.size() < content_length) {
                size_t remaining = content_length - request->body.size();
                ssize_t bytes_read = recv(client_fd, buffer,
                                          std::min(sizeof(buffer), remaining), 0);
                if (bytes_read <= 0) {
                        return false;
                }
                request->body.insert(request->body.end(), buffer, buffer + bytes_read);
        }

        if (request->body.size() > content_length) {
                request->body.resize(content_length);
        }

        return true;
}

static void handle_client(int client_fd, bool redundancy_enabled) {
        Request request;
        if (!read_request(client_fd, &request)) {
                send_response(client_fd, 400, "Bad Request");
                return;
        }

        if (request.method == "GET") {
                handle_get(client_fd, request, redundancy_enabled);
        } else if (request.method == "PUT") {
                handle_put(client_fd, request, redundancy_enabled);
        } else {
                send_response(client_fd, 501, "Not Implemented");
        }
}

static bool arg_present(int argc, char *argv[], const char *flag) {
        for (int i = 1; i < argc; ++i) {
                if (strcmp(argv[i], flag) == 0) {
                        return true;
                }
        }
        return false;
}

static int thread_count(int argc, char *argv[]) {
        for (int i = 1; i + 1 < argc; ++i) {
                if (strcmp(argv[i], "-N") == 0) {
                        int count = atoi(argv[i + 1]);
                        return count > 0 ? count : 4;
                }
        }
        return 4;
}

static std::vector<std::string> positional_args(int argc, char *argv[]) {
        std::vector<std::string> args;
        for (int i = 1; i < argc; ++i) {
                if (strcmp(argv[i], "-r") == 0) {
                        continue;
                }
                if (strcmp(argv[i], "-N") == 0) {
                        ++i;
                        continue;
                }
                args.push_back(argv[i]);
        }
        return args;
}

static unsigned long getaddr(const char *name) {
        struct addrinfo hints;
        struct addrinfo *info;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        if (getaddrinfo(name, NULL, &hints, &info) != 0 || info == NULL) {
                errx(1, "getaddrinfo(): address identification error");
        }

        unsigned long result = ((struct sockaddr_in *)info->ai_addr)->sin_addr.s_addr;
        freeaddrinfo(info);
        return result;
}

static void *worker_thread(void *arg) {
        ThreadData *data = (ThreadData *)arg;

        while (true) {
                pthread_mutex_lock(&queue_mutex);
                while (client_queue.empty()) {
                        pthread_cond_wait(&queue_ready, &queue_mutex);
                }

                int client_fd = client_queue.front();
                client_queue.pop();
                pthread_mutex_unlock(&queue_mutex);

                handle_client(client_fd, data->redundancy_enabled);
                close(client_fd);
        }

        return NULL;
}

int main(int argc, char *argv[]) {
        std::vector<std::string> args = positional_args(argc, argv);
        if (args.empty()) {
                warnx("usage: %s <hostname> <portNumber> [-r] [-N threadCount]", argv[0]);
                return 1;
        }

        const char *address = args[0].c_str();
        unsigned short port = args.size() > 1 ? (unsigned short)strtoul(args[1].c_str(), NULL, 10) : 80;
        bool redundancy_enabled = arg_present(argc, argv, "-r");
        int workers = thread_count(argc, argv);

        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
                err(1, "socket()");
        }

        int reuse = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        struct sockaddr_in servaddr;
        memset(&servaddr, 0, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = getaddr(address);
        servaddr.sin_port = htons(port);

        if (::bind(listen_fd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
                err(1, "bind()");
        }
        if (listen(listen_fd, 500) < 0) {
                err(1, "listen()");
        }

        ThreadData thread_data;
        thread_data.redundancy_enabled = redundancy_enabled;

        std::vector<pthread_t> thread_ids((size_t)workers);
        for (int i = 0; i < workers; ++i) {
                if (pthread_create(&thread_ids[(size_t)i], NULL, worker_thread, &thread_data) != 0) {
                        err(1, "pthread_create()");
                }
        }

        while (true) {
                int client_fd = accept(listen_fd, NULL, NULL);
                if (client_fd == -1) {
                        warn("accept()");
                        continue;
                }

                pthread_mutex_lock(&queue_mutex);
                client_queue.push(client_fd);
                pthread_cond_signal(&queue_ready);
                pthread_mutex_unlock(&queue_mutex);
        }
}
