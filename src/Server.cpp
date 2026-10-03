#include "Server.hpp"
#include <netinet/tcp.h>

const char bad_alloc_res[55] = "-ERR memory limit reached, please resend the request\r\n";

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void Server::safe_close(int fd)
{
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    clients.erase(fd);
}

void sigint_handler(int sig)
{
    (void)sig;
    sigint = 1;
}

void Server::ttl_thread()
{
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (!sigint)
    {
        sleep(config.ttl_sweep_seconds);
        lock.lock();
        cache.check_expired_values();
        lock.unlock();
    }
}

void Server::run()
{
    struct epoll_event events[MAX_EVENTS];
    std::thread ttl(&Server::ttl_thread, this);
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    pid_t serializer_pid = -1;
    Client obj;

    ttl.detach();
    signal(SIGINT, sigint_handler);
    {
        std::lock_guard<std::mutex> lock(mtx);
        cache.Deserialize(config.db_file);
    }
    try
    {
        read_wal();   
    }
    catch(std::exception& e)
    {
        std::cerr << e.what() << '\n';
        exit(1);
    }
    while (!sigint)
    {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;
            try
            {
                if (fd == server_fd)
                {
                    struct sockaddr_in client_addr;
                    socklen_t len = sizeof(client_addr);
                    int conn_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
                    if (conn_fd == -1) {
                        perror("accept failed");
                        continue;
                    }
                    int no_delay = 1;
                    setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay));
                    struct   epoll_event client_event;
                    set_nonblocking(conn_fd);
                    client_event.data.fd = conn_fd;
                    client_event.events = EPOLLIN | EPOLLHUP | EPOLLERR;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &client_event);
                    obj.fd = conn_fd;
                    obj.last_active_time_s = time(NULL);
                    clients.insert(std::make_pair(conn_fd, obj));
                }
                else if (events[i].events & EPOLLIN)
                {
                    parse_request(fd);
                    clients[fd].last_active_time_s = time(NULL);
                    continue ;
                }
                else if (events[i].events & EPOLLOUT)
                {
                    send_response(fd);
                    clients[fd].last_active_time_s = time(NULL);
                    continue;
                }
                else if (events[i].events & (EPOLLHUP | EPOLLERR))
                {
                    if (clients.find(fd) != clients.end())
                        safe_close(fd);
                    continue;
                }
            }
            catch (ERROR &e)
            {
                clients[fd].res_buff = e.what();
                clients[fd].send = clients[fd].res_buff.length();
                clients[fd].sent = send(fd, clients[fd].res_buff.c_str(), clients[fd].send, MSG_NOSIGNAL);
                clients[fd].last_active_time_s = time(NULL);
                if (clients[fd].sent <= 0)
                    safe_close(fd);
                else if (clients[fd].send == clients[fd].sent)
                    make_client_readable(fd,  epoll_fd, clients[fd]);
                continue ;
            }
            catch (std::bad_alloc &e)
            {
                cache.LRU();
                if (clients.find(fd) != clients.end())
                {
                    clients[fd].send = strlen(bad_alloc_res);
                    clients[fd].sent = send(fd, bad_alloc_res, clients[fd].send, MSG_NOSIGNAL);
                    clients[fd].last_active_time_s = time(NULL);
                    if (clients[fd].sent <= 0)
                        safe_close(fd);
                    else if (clients[fd].send == clients[fd].sent)
                        safe_close(fd);
                }
            }
            catch (std::exception &e)
            {
                std::cerr << "ERROR " << e.what() << std::endl;
                if (clients.find(fd) != clients.end())
                    safe_close(fd);
            }
        }
        time_t now = time(NULL);
        for (auto it = clients.begin(); it != clients.end(); )
        {
            if (now - it->second.last_active_time_s >= config.client_timeout_seconds)
            {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, it->second.fd, NULL);
                close(it->second.fd);
                it = clients.erase(it);
                continue ;
            }
            ++it;
        }
        if (std::time(NULL) - last_serialization >= config.snapshot_seconds)
        {
            if (serializer_pid != -1)
                waitpid(serializer_pid, NULL, 0);
            serializer_pid = fork();
            if (!serializer_pid)
            {
                this->cache.Serialize(config.db_file);
                unlink(config.wal_file.c_str());
                exit(0);
            }
            else if (serializer_pid == -1)
            {
                perror("fork failed during snapshot creation");
            }
            last_serialization = std::time(NULL);
        }
    }
    if (serializer_pid != -1)
        waitpid(serializer_pid, NULL, 0);
    lock.lock();
    this->cache.Serialize(config.db_file);
    lock.unlock();
    unlink(config.wal_file.c_str());
};

Server::Server(const RuntimeConfig &cfg): config(cfg)
{
    struct epoll_event ev;
    std::cout << "Starting KV cache server" << std::endl;
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(config.port);
    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, SOMAXCONN);
    set_nonblocking(server_fd);

    std::cout << "Listening on port " << config.port << std::endl;
    epoll_fd = epoll_create1(0);
    ev.events  = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    cmd_func.insert(std::make_pair("GET", &Server::Get));
    cmd_func.insert(std::make_pair("SET", &Server::Set));
    cmd_func.insert(std::make_pair("DEL", &Server::Del));
    cmd_func.insert(std::make_pair("EXISTS", &Server::Exists));
    cmd_func.insert(std::make_pair("EXPIRE", &Server::Expire));
    cmd_func.insert(std::make_pair("TTL", &Server::Ttl));
    cmd_func.insert(std::make_pair("FLUSH", &Server::Flush));
    cmd_func.insert(std::make_pair("TYPE", &Server::Type));
    cmd_func.insert(std::make_pair("QUIT", &Server::Quit));
    cmd_func.insert(std::make_pair("LPOP", &Server::Lpop));
    cmd_func.insert(std::make_pair("RPOP", &Server::Rpop));
    cmd_func.insert(std::make_pair("LPUSH", &Server::Lpush));
    cmd_func.insert(std::make_pair("RPUSH", &Server::Rpush));
    cmd_func.insert(std::make_pair("LRANGE", &Server::Lrange));
    cmd_func.insert(std::make_pair("HSET", &Server::Hset));
    cmd_func.insert(std::make_pair("HGET", &Server::Hget));
    cmd_func.insert(std::make_pair("HGETALL", &Server::Hgetall));
    cmd_func.insert(std::make_pair("HDEL", &Server::Hdel));

    last_serialization = std::time(NULL);
}

Server::~Server()
{
    std::cout << "Shutting down KV cache server" << std::endl;
}
