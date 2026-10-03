#include "Server.hpp"

void lines(str &req_buff, std::vector<str> &cmd, int &lines, int &bytes)
{
    str line;
    size_t indx;
    size_t end;
    size_t start = 0;


    if (lines == 0)
    {
        indx = req_buff.find("\r\n", start);
        if (req_buff[0] != '*')
            throw ERROR("missing number of lines");
        try
        {
            lines = std::stoi(req_buff.substr(1), &end);
        }
        catch(const std::exception& e)
        {
            throw ERROR("unvalid number of lines");
        }
        if (lines < 0 || req_buff.substr(end + 1, 2) != "\r\n")
            throw ERROR("unvalid number of lines");
        start = indx + 2;
    }
    indx = req_buff.find("\r\n", start);
    while (indx != str::npos && cmd.size() != (size_t)lines)
    {
        line = req_buff.substr(start, indx - start);
        if (line[0] != '$')
        {
            if (line.size() != (size_t)bytes)
                throw ERROR("missing length");
            cmd.push_back(line);
            bytes = -1;
        }
        else
        {
            try
            {
                bytes = std::stoi(line.substr(1), &end);
            }
            catch (const std::exception& e)
            {
                throw ERROR("unvalid length");
            }
            if (bytes < 0 || line[end + 1])
                throw ERROR("unvalid length");
        }
        start = indx + 2;
        indx = req_buff.find("\r\n", start);
    }
}

void make_client_writable(int fd,  int epoll_fd)
{
    epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLOUT | EPOLLHUP | EPOLLERR;
    event.data.fd = fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event);
}

void Server::parse_request(int fd)
{
    ssize_t len;
    Client &client = clients[fd];
    size_t old_size = client.req_buff.size();

    client.req_buff.resize(old_size + BUF_SIZE);
    len = recv(fd, &client.req_buff[old_size], BUF_SIZE, MSG_NOSIGNAL);
    if (len == -1)
    {
        client.req_buff.resize(old_size);
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            safe_close(fd);
        return;
    }
    if (len == 0)
        return safe_close(fd);
    client.req_buff.resize(old_size + len);
    if (client.req_buff.find("\r\n") == str::npos)
        return;
    lines(client.req_buff, client.cmd, client.lines, client.bytes);
    client.req_buff = client.req_buff.substr(client.req_buff.find_last_of("\r\n") + 1);
    if ((size_t)client.lines == client.cmd.size())
        make_client_writable(fd, epoll_fd);
}
