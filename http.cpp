#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <err.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdint.h>
#include <time.h>
#include <dirent.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <queue>

// this is for testing, remove before submitting
#include <iostream>
using namespace std;

#define SIZE 100
#define BUFFER_SIZE 8192

//initiliaize locks
pthread_mutex_t mutexQue = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexThread = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexDispatch = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexAvailable = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condThread = PTHREAD_COND_INITIALIZER;
pthread_cond_t condDispatch = PTHREAD_COND_INITIALIZER;

//global variables
int32_t header = -1;
std::queue<int32_t> headerQueue;
int32_t availableThreads;

int isValid(char* inputString) {
        if (strlen(inputString) == 11) {

                char str[100];
                strcpy(str, inputString);

                for (int i = 1; str[i] != '\0'; i++) {
                        if(!isalnum(str[i])) {
                                return 0;
                        }
                }

                return 1;
        } else {
                return 0;
        }
}

// parses arguments to see if redundancy is requested
int isRedundant(int argc, char *argv[]) {
        for (int i = 1; i < argc; i++) {
                if (strcmp(argv[i], "-r") == 0) {
                        *argv[i] = '\0';
                        return 1;
                }
        }
        
        return 0;
}

// parses arguments for thread number
int threadNumber(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
                if (strcmp(argv[i], "-N") == 0) {
                        if (const char *str = argv[i+1]) {
                            int x;
                            sscanf(str, "%d", &x);
                            *argv[i] = '\0';
                            *argv[i+1] = '\0';
                            return x;
                        } else {
                            *argv[i] = '\0';
                            return 4;
                        }
                }
        }   
        
        return 4;
}

// parses arguments to find requested address
char* findAddress(int argc, char *argv[]) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "") != 0) {
            return argv[i];
        }
    }
    return (char*)"localhost";
}

// parses arguments to find requested port
unsigned short findPort(int argc, char *argv[]) {
    int passedAddress = 0;
    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "") != 0) && passedAddress == 0) {
            passedAddress = 1;
        }else if ((strcmp(argv[i], "") != 0) && passedAddress == 1) {
            return (unsigned short) strtoul(argv[i], NULL, 0);
        }
    }
    
    return 80;
}

int compCounter (int cmp12, int cmp13, int cmp23) {
        int count = 0;
        if (cmp12 == 0) {
                count++;
        }
        if (cmp13 == 0) {
                count++;
        }
        if (cmp23 == 0) {
                count++;
        }
        return count;
}

//function that returns the length of a file
long filelength(const char* filpath){
        struct stat st;
        if(stat(filpath, &st))
                return -1;
        return (long) st.st_size;
}

int recv_all(int socket, void *buffer, size_t length) {
        char *ptr = (char*) buffer;
        while (length > 0)  {
                int i = recv(socket, ptr, length, 0);
                if (i < 1) return 0;
                ptr += i;
                length -= i;
        }
        return 1;
}

bool startsWith(const char *pre, const char *str)
{
        size_t lenpre = strlen(pre),
               lenstr = strlen(str);
        return lenstr < lenpre ? false : memcmp(pre, str, lenpre) == 0;
}

int isForbidden(char* path) {
        if(access(path, F_OK) == 0) {
                int fd = open(path, O_RDWR);
                if(fd == -1) {
                        return 1;
                }
                close(fd);
        }
        return 0;
}

// copies contents of file located at srcpath, and
// makes a new file at destpath
int copyTo(char* destpath, char* srcpath) {
        int dest_fd, src_fd;
        int32_t readBytes = 0;
        size_t length = filelength(srcpath);
        char *buffer = (char*)malloc(length);

        src_fd = open(srcpath, O_RDWR);
        
        if (src_fd > -1) {
                readBytes = read(src_fd,buffer,length);

                dest_fd = open(destpath, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU);
        }



        if (dest_fd == -1 || src_fd == -1) {
                return -1;
        }

        while(readBytes > 0) {
                int32_t writeTo = write(dest_fd, buffer, readBytes);
                readBytes = read(src_fd, buffer, length);
        }

        close(dest_fd);
        close(src_fd);
        free(buffer);
        return 0;
}

void doPut(char file[15], int header, int contentLength, int redundancyStatus = 0) {

        char *token;
        char head[100];
        char *badRequest = (char*)"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        char *okRequest = (char*)"HTTP/1.1 200 OK\r\nContent-Length:";
        char fileName[BUFFER_SIZE];

        // send error if file name is not long enough
        if (!isValid(file)) {
                sprintf(head, badRequest, strlen(badRequest), 0);
                send(header, head, strlen(head), 0);
                return;
        }

        // get token
        strcpy(fileName, file);
        token = strtok(fileName, "/");

        // send file contents to buffer
        char *buffer = (char*)malloc(contentLength);
        recv_all(header, buffer, contentLength);

        if (redundancyStatus == 1) {
                int copy1_fd;
                int copy2_fd;
                int copy3_fd;

                // assign copy file discriptors.
                char *copy1Token = new char[50];
                char *copy2Token = new char[50];
                char *copy3Token = new char[50];

                strcpy(copy1Token, "copy1/");
                strcpy(copy2Token, "copy2/");
                strcpy(copy3Token, "copy3/");

                strcat(copy1Token, token);
                strcat(copy2Token, token);
                strcat(copy3Token, token);

                // chdir("/copy1");

                if((copy1_fd = open(copy1Token, O_WRONLY | O_CREAT | O_TRUNC,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
                         perror("Cannot open output file\n");
                         exit(1);
                }

                if((copy2_fd = open(copy2Token, O_WRONLY | O_CREAT | O_TRUNC,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
                         perror("Cannot open output file\n");
                         exit(1);
                }

                if((copy3_fd = open(copy3Token, O_WRONLY | O_CREAT | O_TRUNC,
                 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
                         perror("Cannot open output file\n");
                         exit(1);
                }
                
                write(copy1_fd, buffer, contentLength);
                write(copy2_fd, buffer, contentLength);
                write(copy3_fd, buffer, contentLength);

                // buffer to new files

                close(copy1_fd);
                close(copy2_fd);
                close(copy3_fd);

                free(buffer);
        } else {
                // create new file
                int dest_fd;
                // dest_fd = open(token, O_CREAT | O_RDWR, S_IRWXU);
                if((dest_fd = open(token, O_WRONLY | O_CREAT | O_TRUNC,
                                S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
                        perror("Cannot open output file\n");
                        exit(1);
                }

                write(dest_fd, buffer, contentLength);

                // free, close
                free(buffer);
                close(dest_fd);            
        }

        // create new file
        int dest_fd;
        // dest_fd = open(token, O_CREAT | O_RDWR, S_IRWXU);
        if((dest_fd = open(token, O_WRONLY | O_CREAT | O_TRUNC,
                           S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
                perror("Cannot open output file\n");
                exit(1);
        }

        // return okRequest
        sprintf(head, "%s %d\r\n\r\n",  okRequest, contentLength);
        send(header, head, strlen(head), 0);

        return;

}

void doGet(char file[15], int header, int redundancyStatus = 0) {
        int fd;
        char *token;
        char *buffer = (char*)malloc(32768);
        char *badRequest = (char*)"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        char *fileNotFound = (char*)"HTTP/1.1 404 File Not Found\r\nContent-Length: 0\r\n\r\n";
        char *okRequest = (char*)"HTTP/1.1 200 OK\r\nContent-Length:";
        char *forbiddenRequest = (char*)"HTTP/1.1 403 Forbidden\r\nContent-Length:";
        char head[100];
        char fileName[BUFFER_SIZE];

        strcpy(fileName, file);
        token = strtok(fileName, "/");
        std::string tokenString(token);
        long contentLength = filelength(token);
        if(tokenString == "b") {

                // get timestamp
                int timestamp = (int)time(NULL);

                // convert timestamp to string
                char timestampStr[10];
                sprintf(timestampStr,"%d", timestamp);

                // make timestamp directory
                char dirName[100];

                strcpy(dirName, "backup-");
                strcat(dirName, timestampStr);

                mkdir(dirName, S_IRWXU);

                // iterate through files in directory
                DIR *dir;
                struct dirent *sd;
                dir = opendir(".");
                if (dir == NULL) {
                        printf("Error! Unable to open directory.\n");
                        sprintf(head, "HTTP/1.1 500 Internal Server Error\"3\"\r\nContent Length: 0\r\n\r\n");
                        send(header, head, strlen(head), 0);
                        exit(1);
                }

                char dashToken[11];
                char pathStr[100];
                strcpy(pathStr, dirName);
                while((sd=readdir(dir)) != NULL) {
                        strcpy(dashToken, "/");
                        if (isValid(strcat(dashToken, sd->d_name))) {
                                char tempDest[100];
                                strcpy(tempDest, pathStr);
                                strcat(tempDest, "/");
                                strcat(tempDest, sd->d_name);

                                copyTo(tempDest, sd->d_name);

                        }

                }
                closedir(dir);
                sprintf(head, "%s %ld\r\n\r\n",  okRequest, (long)0);
                send(header, head, strlen(head), 0);
                return;

        }
        if(tokenString == "r") {
                // find most recent timestamp

                if(strcmp(file, "/r") == 0) {
                        DIR *dir;
                        struct dirent *sd;
                        dir = opendir(".");
                        if (dir == NULL) {
                                printf("Error! Unable to open directory.\n");
                                sprintf(head, badRequest, strlen(badRequest), 0);
                                send(header, head, strlen(head), 0);
                                exit(1);
                        }

                        int largest = 0;

                        while((sd=readdir(dir)) != NULL) {
                                if(startsWith("backup-", sd->d_name)) {
                                        char *last = strrchr(sd->d_name, '-') + 1;

                                        if (atoi(last) > largest) {
                                                largest = atoi(last);
                                        }
                                }
                        }

                        // convert int largest back to path
                        char backupFolderPath[100];
                        strcpy(backupFolderPath, "backup-");
                        char numBuffer[15];
                        sprintf(numBuffer, "%d", largest);
                        strcat(backupFolderPath, numBuffer);

                        printf("forbidden status: %d\n", isForbidden(backupFolderPath));
                        if (isForbidden(backupFolderPath)) {
                                sprintf(head, forbiddenRequest, strlen(forbiddenRequest), 0);
                                send(header, head, strlen(head), 0);
                                exit(1);                               
                        }

                        // access files in backup
                        dir = opendir(backupFolderPath);
                        if (dir == NULL) {
                                printf("Error! Unable to open directory.\n");
                                sprintf(head, fileNotFound, strlen(fileNotFound), 0);
                                send(header, head, strlen(head), 0);
                                exit(1);
                        }

                        char dashToken[25];
                        while((sd = readdir(dir)) != NULL) {
                                strcpy(dashToken, "/");
                                if (isValid(strcat(dashToken, sd->d_name))) {
                                        // copy file to main directory
                                        char srcPath[100];
                                        strcpy(srcPath, backupFolderPath);
                                        strcat(srcPath, "/");
                                        strcat(srcPath, sd->d_name);

                                        copyTo(sd->d_name, srcPath);
                                }
                        }
                } else {
                        char *number = strrchr(file, '/') + 1;

                        // check if requested backup exists
                        char backupFolderPath[100];
                        strcpy(backupFolderPath, "backup-");
                        strcat(backupFolderPath, number);

                        DIR *dir;
                        struct dirent *sd;

                        if (isForbidden(backupFolderPath)) {
                                sprintf(head, forbiddenRequest, strlen(forbiddenRequest), 0);
                                send(header, head, strlen(head), 0);
                                exit(1);                               
                        }

                        dir = opendir(backupFolderPath);
                        if (dir == NULL) {
                                printf("Error! Unable to open directory.\n");
                                sprintf(head, badRequest, strlen(badRequest), 0);
                                send(header, head, strlen(head), 0);
                                exit(1);
                        }

                        char dashToken[25];
                        while((sd = readdir(dir)) != NULL) {
                                strcpy(dashToken, "/");
                                if (isValid(strcat(dashToken, sd->d_name))) {
                                        // copy file to main directory
                                        char srcPath[100];
                                        strcpy(srcPath, backupFolderPath);
                                        strcat(srcPath, "/");
                                        strcat(srcPath, sd->d_name);

                                        copyTo(sd->d_name, srcPath);
                                }
                        }

                }

                long contentLength = 0;

                sprintf(head, "%s %ld\r\n\r\n",  okRequest, (long)0);
                send(header, head, strlen(head), 0);
                return;

        }
        if(tokenString =="l") {
                char returnBuffer[1000];
                memset(returnBuffer, '\0', 1000);
                int counter = 0;
                DIR *dir;
                struct dirent *sd;
                dir = opendir(".");
                if (dir == NULL) {
                        printf("Error! Unable to open directory.\n");
                        sprintf(head, "HTTP/1.1 500 Internal Server Error\"3\"\r\nContent Length: 0\r\n\r\n");
                        send(header, head, strlen(head), 0);
                        exit(1);
                }
                while((sd=readdir(dir)) != NULL) {
                        if(startsWith("backup-", sd->d_name)) {

                                char *last = strrchr(sd->d_name, '-') + 1;
                                strcat(returnBuffer, last);
                                strcat(returnBuffer, "\n");

                                counter++;
                        }
                }
                // get length of used returnBuffer
                int contentLength = 0;
                for (int i = 0; returnBuffer[i] != '\0'; i++) {
                        contentLength++;
                }

                sprintf(head, "%s %d\r\n\r\n",  okRequest, contentLength);
                send(header, head, strlen(head), 0);
                
                int32_t readBytes = contentLength;
                while (readBytes > 0) {
                        int32_t writeTo = write(header, returnBuffer, readBytes);
                        readBytes = readBytes - writeTo;
                }
                free(buffer);
                close(fd);
                return;
        }
        

        //make sure that file length is long enough
        if(!isValid(file)) {
                sprintf(head, badRequest, strlen(badRequest), 0);
                send(header, head, strlen(head), 0);
                return;
        }

        if (redundancyStatus == 1) {

                // check that the three files exist
                char *copy1Token = new char[50];
                char *copy2Token = new char[50];
                char *copy3Token = new char[50];

                strcpy(copy1Token, "copy1/");
                strcpy(copy2Token, "copy2/");
                strcpy(copy3Token, "copy3/");

                strcat(copy1Token, token);
                strcat(copy2Token, token);
                strcat(copy3Token, token);


                if ((access(copy1Token, F_OK) != 0) || (access(copy2Token, F_OK) != 0)
                        || (access(copy3Token, F_OK) != 0)) {
                                sprintf(head, fileNotFound, strlen(fileNotFound), 0);
                                send(header, head, strlen(head), 0);
                                return; 
                        }
                
                

                // set file descriptors
                int copy1fd = open(copy1Token, O_RDWR);
                int copy2fd = open(copy2Token, O_RDWR);
                int copy3fd = open(copy3Token, O_RDWR);

                // write file contents to their respective buffers
                char *copy1Buffer = (char*)malloc(32768);
                char *copy2Buffer = (char*)malloc(32768);
                char *copy3Buffer = (char*)malloc(32768);

                int32_t readBytes1 = read(copy1fd,copy1Buffer,32768);
                int32_t readBytes2 = read(copy2fd,copy2Buffer,32768);
                int32_t readBytes3 = read(copy3fd,copy3Buffer,32768);

                // create comparators; 0 = both buffers are equal
                int cmp12 = strcmp(copy1Buffer, copy2Buffer);
                int cmp13 = strcmp(copy1Buffer, copy3Buffer);
                int cmp23 = strcmp(copy2Buffer, copy3Buffer);

                int equalCount = compCounter(cmp12, cmp13, cmp23);

                printf("equalCount: %d\n", equalCount);

                // all three files have the same file
                if (equalCount == 3) {

                        // send okay message
                        sprintf(head, "%s %ld\r\n\r\n",  okRequest, filelength(copy1Token));
                        send(header, head, strlen(head), 0);

                        // if all files are the same, return first file contents
                        printf("the same\n");
                        while(readBytes1 > 0) {
                                //while readBytes still has stuff in it, write its contents to output
                                // write writes up to (3) bytes from (2) to (1)
                                int32_t writeTo = write(header, copy1Buffer, readBytes1);
                                readBytes1 = read(copy1fd, copy1Buffer, 32768);
                                //checks to see if there are anymore bytes in fd
                        }
                } else if(equalCount == 1) {
                        // only two files match
                        if ((cmp12 == 0) || (cmp13 == 0)) {
                                // if either copy1 and copy2 match, or copy1 and 
                                // copy3 match, return contents of copy1

                                // send okay message
                                sprintf(head, "%s %ld\r\n\r\n",  okRequest, filelength(copy1Token));
                                send(header, head, strlen(head), 0);

                                // return first file contents
                                printf("one is different, printing file1\n");
                                while(readBytes1 > 0) {
                                        //while readBytes still has stuff in it, write its contents to output
                                        // write writes up to (3) bytes from (2) to (1)
                                        int32_t writeTo = write(header, copy1Buffer, readBytes1);
                                        readBytes1 = read(copy1fd, copy1Buffer, 32768);
                                        //checks to see if there are anymore bytes in fd
                                }
                        } else {
                                // this means cmp23 == 0 and the contents of file 3 should be printed
                                // send okay message
                                sprintf(head, "%s %ld\r\n\r\n",  okRequest, filelength(copy3Token));
                                send(header, head, strlen(head), 0);

                                // return first file contents
                                printf("one is different, printing file3\n");
                                while(readBytes3 > 0) {
                                        //while readBytes still has stuff in it, write its contents to output
                                        // write writes up to (3) bytes from (2) to (1)
                                        int32_t writeTo = write(header, copy3Buffer, readBytes3);
                                        readBytes3 = read(copy1fd, copy3Buffer, 32768);
                                        //checks to see if there are anymore bytes in fd
                                }
                        }
                } else {
                        // all files are different
                        sprintf(head, "HTTP/1.1 500 Internal Server Error\"3\"\r\nContent Length: 0\r\n\r\n");
                        send(header, head, strlen(head), 0);
                }

                

                free(copy1Buffer);
                free(copy2Buffer);
                free(copy3Buffer);

                close(copy1fd);
                close(copy2fd);
                close(copy3fd);

        } else {
                             
                char *buffer = (char*)malloc(32768);

                //check if file exists
                if(access(token, F_OK) == 0) {
                        //from dog to open file
                        fd = open(token, O_RDWR);
                        //see if the file doesnt exsist
                        if(fd == -1) {
                                sprintf(head, forbiddenRequest, strlen(forbiddenRequest), 0);
                                send(header, head, strlen(head), 0);
                                return;
                        }
                }else{
                        //it doesnt exsist
                        sprintf(head, fileNotFound, strlen(fileNotFound), 0);
                        send(header, head, strlen(head), 0);
                        return;
                }
                sprintf(head, "%s %ld\r\n\r\n",  okRequest, contentLength);
                send(header, head, strlen(head), 0);
                //print from dog
                // read reads up to (3) bytes from (1) to (2)
                int32_t readBytes = read(fd,buffer,32768);
                while(readBytes > 0) {
                        //while readBytes still has stuff in it, write its contents to output
                        // write writes up to (3) bytes from (2) to (1)
                        int32_t writeTo = write(header, buffer, readBytes);
                        readBytes = read(fd, buffer, 32768);
                        //checks to see if there are anymore bytes in fd
                }
                free(buffer);
                close(fd);
        }


        return;

}


void parse_header(int header, int redundancyStatus) {
        char *buffer = (char*)malloc(32768);
        char command[10];
        char file[15];
        char head[55];
        int32_t readHeader = read(header,buffer,32768);
        int contentLength;
        char *lengthPtr;

        sscanf(buffer, "%s %s", command, file);
        if(strcmp(command,"GET")==0) {
                doGet(file, header);
        }else if(strcmp(command,"PUT")==0) {
                lengthPtr = strstr(buffer, "Content-Length:");
                sscanf(lengthPtr, "Content-Length: %d", &contentLength);
                doPut(file, header, contentLength);
        } else {
                sprintf(head, "HTTP/1.1 500 Internal Server Error\"3\"\r\nContent Length: 0\r\n\r\n");
                send(header, head, strlen(head), 0);
        }
        free(buffer);
        return;
}

/*
   getaddr returns the numerical representation of the address
   identified by *name* as required for an IPv4 address represented
   in a struct sockaddr_in.
 */
//code from danial posted on piazza
unsigned long getaddr(char *name) {
        unsigned long res;
        struct addrinfo hints;
        struct addrinfo* info;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        if (getaddrinfo(name, NULL, &hints, &info) != 0 || info == NULL) {
                char msg[] = "getaddrinfo(): address identification error\n";
                write(STDERR_FILENO, msg, strlen(msg));
                exit(1);
        }
        res = ((struct sockaddr_in*) info->ai_addr)->sin_addr.s_addr;
        freeaddrinfo(info);
        return res;
}

struct thread_data{
        int redundancyStatus;
};

// thread function that does stuff
void* worker_thread(void* arg) {

        struct thread_data *my_data;

        my_data = (struct thread_data *) arg;

        int redundancyStatus = my_data->redundancyStatus;

        long not_used = (long)header;
        not_used++;

        while(1) {
                pthread_cond_wait(&condThread, &mutexThread);
                pthread_mutex_lock(&mutexAvailable);
                availableThreads--;

                pthread_mutex_unlock(&mutexAvailable);
                pthread_mutex_lock(&mutexQue);

                int32_t front = headerQueue.front();
                printf("%d\n", front);

                headerQueue.pop();
                pthread_mutex_unlock(&mutexQue);

                parse_header(front, redundancyStatus);

                pthread_mutex_lock(&mutexAvailable);
                availableThreads++;
                pthread_mutex_unlock(&mutexAvailable);
                pthread_cond_signal(&condDispatch);
        }
        
}

int main(int argc, char *argv[]) {

        if(argc < 2) {
                warn("%s", "Please put an address");
                return 0;
        }

        // redundancy
        int redundancyStatus = isRedundant(argc, argv);
        // thread number
        int threadCount = threadNumber(argc, argv);
        // address
        char* address = findAddress(argc, argv);
        // port
        unsigned short port = findPort(argc, argv);

        struct sockaddr_in servaddr;
        memset(&servaddr, 0, sizeof(servaddr));

        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) err(1, "socket()");

        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = getaddr(address);
        servaddr.sin_port = htons(port);

        if(bind(listen_fd, (struct sockaddr*) &servaddr, sizeof(servaddr)) < 0) err(1, "bind()");

        if(listen(listen_fd, 500) < 0) err(1, "listen()");

        struct thread_data thread_data_array[threadCount];

        pthread_t *threadId = (pthread_t *)malloc(sizeof(pthread_t) * threadCount);
        for (int i = 0; i < threadCount; i++) { 
                thread_data_array[i].redundancyStatus = redundancyStatus;
                int wThreads = pthread_create(&threadId[i], NULL, worker_thread, &thread_data_array[i]);
                printf("thread %d created. Waiting\n", i);
                if (wThreads < 0) {
                        warn("%s", "Error while using pthread_create");
                        return 1;
                }
        }
        availableThreads = threadCount;

        while(1) {
                printf("waiting...\n");
                int header = accept(listen_fd, NULL, NULL);

                if (header == -1) {
                        warn("%s", "accept error");
                        return 1;
                }

                pthread_mutex_lock(&mutexQue);
                headerQueue.push(header);
                pthread_mutex_unlock(&mutexQue);
                
                pthread_mutex_lock(&mutexAvailable);

                while (availableThreads == 0) {
                        //sleep while no available threads
                        pthread_mutex_unlock(&mutexAvailable);
                        pthread_cond_wait(&condDispatch, &mutexDispatch);
                        pthread_mutex_lock(&mutexAvailable);
                }
                pthread_mutex_unlock(&mutexAvailable);
                pthread_cond_signal(&condThread);
        }

}
