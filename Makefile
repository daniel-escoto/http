CXX ?= g++
CXXFLAGS ?= -std=c++11 -Wall -Wextra -pedantic

all: http

http: http.cpp
	$(CXX) $(CXXFLAGS) http.cpp -o http

clean:
	rm -f http
