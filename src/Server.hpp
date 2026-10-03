#pragma once

#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <ctime>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <fstream>
#include <vector>
#include <list>
#include <exception>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>

#include "Cache.hpp"

#define MAX_EVENTS  64
#define BUF_SIZE    4096

constexpr int DEFAULT_PORT = 8080;
constexpr int DEFAULT_TTL_SWEEP_SECONDS = 60;
constexpr int DEFAULT_SNAPSHOT_SECONDS = 30;
constexpr int DEFAULT_CLIENT_TIMEOUT_SECONDS = 10;
constexpr std::uint64_t DEFAULT_MEMORY_LIMIT_MB = 16;
constexpr const char *DEFAULT_DB_FILE = "custom.db";
constexpr const char *DEFAULT_WAL_FILE = "wal.log";

extern std::atomic<bool> sigint;

class ERROR: public std::exception
{
    private:
        std::string msg;
    public:
        ERROR(str m)
        {
            msg = m;
        }
        ~ERROR() throw()
        {}
        virtual const char *what() const throw()
        {
            return msg.c_str();
        }
};

struct Client
{
    str res_buff;
    str req_buff;
    std::vector <str> cmd;
    long long last_active_time_s;
    int lines;
    int bytes;
    int send;
    int sent;
    int fd;
    bool bad_alloc;
    bool quit;

    Client()
    {
        lines = 0;
        bytes = -1;
        send = 0;
        sent = 0;
        quit = false;
    }
    void clear()
    {
        lines = 0;
        bytes = -1;
        send = 0;
        sent = 0;
        quit = false;
        res_buff.clear();
        cmd.clear();
    }
};

void make_client_readable(int fd,  int epoll_fd, Client &cl);

struct RuntimeConfig
{
    int port;
    int ttl_sweep_seconds;
    int snapshot_seconds;
    int client_timeout_seconds;
    std::string db_file;
    std::string wal_file;

    RuntimeConfig():
        port(DEFAULT_PORT),
        ttl_sweep_seconds(DEFAULT_TTL_SWEEP_SECONDS),
        snapshot_seconds(DEFAULT_SNAPSHOT_SECONDS),
        client_timeout_seconds(DEFAULT_CLIENT_TIMEOUT_SECONDS),
        db_file(DEFAULT_DB_FILE),
        wal_file(DEFAULT_WAL_FILE)
    {}
};

class Server
{
    std::unordered_map <int, Client> clients;
    std::unordered_map <str, void(Server::*)(int)> cmd_func;
    Cache cache;
    RuntimeConfig config;
    std::mutex mtx;
    int server_fd;
    int epoll_fd;
    int wal_fd;
    long long last_serialization;
    public:
        void run();
        void parse_request(int fd);
        void send_response(int fd);
        void safe_close(int fd);
        void Set(int fd);
        void Get(int fd);
        void Del(int fd);
        void Exists(int fd);
        void Expire(int fd);
        void Ttl(int fd);
        void Flush(int fd);
        void Type(int fd);
        void Quit(int fd);
        void Lpush(int fd);
        void Rpush(int fd);
        void Lpop(int fd);
        void Rpop(int fd);
        void Lrange(int fd);
        void Hset(int fd);
        void Hget(int fd);
        void Hgetall(int fd);
        void Hdel(int fd);
        void ttl_thread();
        int Wal(std::vector <str> &cmd);
        void read_wal();
        size_t read_wal_cmd_lines(str &buff, int &lines, int &bytes, std::vector<str> &cmd);
        bool exec_wal(std::vector <str> &cmd);
        explicit Server(const RuntimeConfig &cfg);
        ~Server();
};

