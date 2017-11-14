#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <system_error>

#include "common/logger.h"
#include "common/selectabletimer.h"

namespace swss {

SelectableTimer::SelectableTimer(const timespec& interval)
    : m_zero({{0, 0}, {0, 0}})
{
    // Create the timer
    m_tfd = timerfd_create(CLOCK_REALTIME, 0);
    if (m_tfd == -1)
    {
        SWSS_LOG_ERROR("failed to create timerfd, errno: %s", strerror(errno));
        throw std::runtime_error("failed to create timerfd");
    }
    setInterval(interval);
}

SelectableTimer::~SelectableTimer()
{
    int err;

    do
    {
        err = close(m_tfd);
    }
    while(err == -1 && errno == EINTR);
}

void SelectableTimer::start()
{
    // Set the timer interval and the timer is automatically started
    int rc = timerfd_settime(m_tfd, 0, &m_interval, NULL);
    if (rc == -1)
    {
        SWSS_LOG_ERROR("failed to set timerfd, errno: %s", strerror(errno));
        throw std::runtime_error("failed to set timerfd");
    }
}

void SelectableTimer::stop()
{
    // Set the timer interval and the timer is automatically started
    int rc = timerfd_settime(m_tfd, 0, &m_zero, NULL);
    if (rc == -1)
    {
        SWSS_LOG_ERROR("failed to set timerfd to zero, errno: %s", strerror(errno));
        throw std::runtime_error("failed to set timerfd to zero");
    }
}

void SelectableTimer::setInterval(const timespec& interval)
{
    // The initial expiration and intervals to caller specified
    m_interval.it_value = interval;
    m_interval.it_interval = interval;
}

void SelectableTimer::addFd(fd_set *fd)
{
    FD_SET(m_tfd, fd);
}

int SelectableTimer::readCache()
{
    return Selectable::NODATA;
}

void SelectableTimer::readMe()
{
    uint64_t r;

    ssize_t s;
    do
    {
        // Read the timefd so it will be reset
        s = read(m_tfd, &r, sizeof(uint64_t));
    }
    while(s == -1 && errno == EINTR);

    if (s != sizeof(uint64_t))
    {
        SWSS_LOG_ERROR("read failed, errno: %s", strerror(errno));

        throw std::runtime_error("read failed");
    }
}

bool SelectableTimer::isMe(fd_set *fd)
{
    return FD_ISSET(m_tfd, fd);
}

}
