#pragma once

#include <fstream>
#include <string>
#include <mutex>
#include <chrono>
#include <ctime>
#include <sstream>
#include <iomanip>

class DebugLog
{
public:
    enum LogLevel
    {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    };

    static DebugLog &getInstance()
    {
        static DebugLog instance;
        return instance;
    }

    void init(const std::string &log_file)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        log_file_.open(log_file, std::ios::out | std::ios::app);
    }

    void log(LogLevel level, const std::string &module, const std::string &message)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!log_file_.is_open())
            return;

        auto now = std::chrono::system_clock::now();
        auto now_time = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

        std::stringstream ss;
        ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S")
           << '.' << std::setfill('0') << std::setw(3) << ms.count()
           << " [" << getLevelString(level) << "] "
           << "[" << module << "] "
           << message
           << std::endl;

        log_file_ << ss.str();
        log_file_.flush();
    }

    void debug(const std::string &module, const std::string &message)
    {
        log(DEBUG, module, message);
    }

    void info(const std::string &module, const std::string &message)
    {
        log(INFO, module, message);
    }

    void warning(const std::string &module, const std::string &message)
    {
        log(WARNING, module, message);
    }

    void error(const std::string &module, const std::string &message)
    {
        log(ERROR, module, message);
    }

private:
    DebugLog() = default;
    ~DebugLog()
    {
        if (log_file_.is_open())
        {
            log_file_.close();
        }
    }

    DebugLog(const DebugLog &) = delete;
    DebugLog &operator=(const DebugLog &) = delete;

    std::string getLevelString(LogLevel level)
    {
        switch (level)
        {
        case DEBUG:
            return "DEBUG";
        case INFO:
            return "INFO";
        case WARNING:
            return "WARN";
        case ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
        }
    }

    std::ofstream log_file_;
    std::mutex mutex_;
};