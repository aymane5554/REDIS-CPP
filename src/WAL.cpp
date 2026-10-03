#include "Server.hpp"

bool Server::exec_wal(std::vector <str> &cmd)
{
    size_t end_index;

    try
    {
        if (cmd[0] == "SET" && cmd.size() == 3)
            cache.Set(cmd);
        else if (cmd[0] == "HSET" && cmd.size() == 4)
            cache.Hset(cmd);
        else if (cmd[0] == "HDEL" && cmd.size() == 3)
            cache.Hdel(cmd);
        else if (cmd[0] == "DEL" && cmd.size() == 2)
            cache.Del(cmd[1]);
        else if (cmd[0] == "LPUSH" && cmd.size() >= 3)
            cache.Lpush(cmd);
        else if (cmd[0] == "RPUSH" && cmd.size() >= 3)
            cache.Rpush(cmd);
        else if (cmd[0] == "LPOP" && cmd.size() == 2)
            cache.Lpop(cmd[1]);
        else if (cmd[0] == "RPOP" && cmd.size() == 2)
            cache.Rpop(cmd[1]);
        else if (cmd[0] == "FLUSH" && cmd.size() == 1)
            cache.Flush();
        else if (cmd[0] == "EXPIRE" && cmd.size() == 3)
        {
            size_t seconds = std::stoull(cmd[2].c_str(), &end_index, 10);
            if (cmd[2][end_index] != '\0')
                throw std::runtime_error("Invalid WAL format");
            cache.Expire(cmd[1], seconds);
        }
        else
            throw std::runtime_error("Invalid WAL format");
    }
    catch (std::bad_alloc &e)
    {
        return false;
    }
    return true;
}

int Server::Wal(std::vector <str> &cmd)
{
    str data = "*" + std::to_string(cmd.size()) + "\r\n";
    for (size_t i = 0; i < cmd.size(); i++)
    {
        data += "$" + std::to_string(cmd[i].size()) + "\r\n";
        data += cmd[i] + "\r\n";
    }
    if (wal_fd == -1)
        return -1;
    write(wal_fd, data.c_str(), data.size());
    return 0;
}

size_t Server::read_wal_cmd_lines(str &buff, int &lines, int &bytes, std::vector<str> &cmd)
{
    str line;
    size_t indx;
    size_t end;
    size_t start = 0;
    size_t offset = 0;

    if (lines == 0)
    {
        indx = buff.find("\r\n", start);
        if (buff[0] != '*')
            throw ERROR("missing number of lines");
        try
        {
            lines = std::stoi(buff.substr(1), &end);
        }
        catch(const std::exception& e)
        {
            throw ERROR("unvalid number of lines");
        }
        if (lines < 0 || buff.substr(end + 1, 2) != "\r\n")
            throw ERROR("unvalid number of lines");
        start = indx + 2;
    }
    indx = buff.find("\r\n", start);
    while (indx != str::npos && cmd.size() != (size_t)lines)
    {
        line = buff.substr(start, indx - start);
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
        indx = buff.find("\r\n", start);
    }
    offset = start;  
    return offset;
}

void Server::read_wal()
{
    std::vector<str> cmd;
    size_t len;
    int lines = 0;
    int bytes = -1;
    size_t offset = 0;
    wal_fd = open(config.wal_file.c_str(), O_CREAT | O_APPEND | O_RDWR, 0777);
    int fd = wal_fd;
    str str_buff;
    char buff[BUF_SIZE];

    if (fd == -1)
    {
        if (errno == ENOENT)
        {
            std::cout << "WAL file not found" << std::endl;
            return ;
        }
        throw std::runtime_error("Failed to open WAL file");
    }
    len = read(fd, buff, BUF_SIZE);
    while (len > 0 || !str_buff.empty())
    {
        if (len <= 0 && str_buff.find("\r\n") == str::npos)
            break ;
        str_buff.append(buff, len);
        offset = read_wal_cmd_lines(str_buff, lines, bytes, cmd);
        str_buff.erase(0, offset);
        if ((size_t)lines == cmd.size())
        {
            exec_wal(cmd);
            cmd.clear();
            lines = 0;
            bytes = -1;
        }
        len = read(fd, buff, BUF_SIZE);
    }
}
