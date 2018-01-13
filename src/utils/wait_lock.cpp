/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include <random>
#include "wait_lock.h"
#include <stdio.h>
#include "utils_include.h"

namespace phxpaxos
{

WaitLock :: WaitLock() 
    :m_bIsLockUsing(false), m_iWaitLockCount(0), m_iMaxWaitLockCount(-1),
    m_iLockUseTimeSum(0), m_iAvgLockUseTime(0), m_iLockUseTimeCount(0),
    m_iRejectRate(0), m_iLockWaitTimeThresholdMS(-1)
{
}

WaitLock :: ~WaitLock()
{
}

//判断是否可以获取锁 private
bool WaitLock :: CanLock()
{
    if (m_iMaxWaitLockCount != -1
            && m_iWaitLockCount >= m_iMaxWaitLockCount) 
    {
        //to much lock waiting
        return false;
    }
    
    if (m_iLockWaitTimeThresholdMS == -1)
    {
        //没有设置lock等待时间阈值，直接返回成功
        return true;
    }

    //设置了m_iLockWaitTimeThresholdMS， 根据拒绝率来随机拒绝掉某些请求，达到过载保护的作用
    static std::default_random_engine e_rand;
    return ((int)(e_rand() % 100)) >= m_iRejectRate;
}

//刷新锁的拒绝率
void WaitLock :: RefleshRejectRate(const int iUseTimeMs)
{
    if (m_iLockWaitTimeThresholdMS == -1)
    {
        return;
    }
    
    //这个其实需要刷新拒绝率，这里设置了一个m_iLockWaitTimeThresholdMS
    //平均每次锁的时间，当然了，没有设置的话就直接跳过 ，拒绝率永远为0
    //在锁执行了250次之后进行统计，每次平均获取锁用时超过了m_iLockWaitTimeThresholdMS， 将拒绝率增加3%，并且重置重新统计。
    //每次平均获取锁用时低于m_iLockWaitTimeThresholdMS， 将拒绝率减少3%
    //拒绝率在CanLock函数中使用随机数的方式表示是否拒绝
    m_iLockUseTimeSum += iUseTimeMs;
    m_iLockUseTimeCount++;
    if (m_iLockUseTimeCount >= WAIT_LOCK_USERTIME_AVG_INTERVAL)
    {
        m_iAvgLockUseTime = m_iLockUseTimeSum / m_iLockUseTimeCount;
        m_iLockUseTimeSum = 0;
        m_iLockUseTimeCount = 0;

        if (m_iAvgLockUseTime > m_iLockWaitTimeThresholdMS)
        {
            if (m_iRejectRate != 98)
            {
                m_iRejectRate = m_iRejectRate + 3 > 98 ? 98 : m_iRejectRate + 3;
            }
        }
        else
        {
            if (m_iRejectRate != 0)
            {
                m_iRejectRate = m_iRejectRate - 3 < 0 ? 0 : m_iRejectRate - 3;
            }
        }
    }
}

//设置最大等待锁的个数
void WaitLock :: SetMaxWaitLogCount(const int iMaxWaitLockCount)
{
    m_iMaxWaitLockCount = iMaxWaitLockCount;
}

//设置平均锁等待时间的阈值，超过这个阈值，有几率失败
void WaitLock :: SetLockWaitTimeThreshold(const int iLockWaitTimeThresholdMS)
{
    m_iLockWaitTimeThresholdMS = iLockWaitTimeThresholdMS;
}

//加锁
bool WaitLock :: Lock(const int iTimeoutMs, int & iUseTimeMs)
{
    uint64_t llBeginTime = Time::GetSteadyClockMS();

    m_oSerialLock.Lock();
    if (!CanLock())
    {
        //太多人在等待了，返回错误出去
        //printf("reject, now rate %d\n", m_iRejectRate);
        iUseTimeMs = 0;
        m_oSerialLock.UnLock();
        return false;
    }

    m_iWaitLockCount++;
    bool bGetLock = true;;

    while (m_bIsLockUsing)
    {
        if (iTimeoutMs == -1)
        {
            m_oSerialLock.WaitTime(1000);
            continue;
        }
        else
        {
            if (!m_oSerialLock.WaitTime(iTimeoutMs))
            {
                //lock timeout
                bGetLock = false;
                break;
            }
        }
    }

    m_iWaitLockCount--;

    uint64_t llEndTime = Time::GetSteadyClockMS();
    iUseTimeMs = llEndTime > llBeginTime ? (int)(llEndTime - llBeginTime) : 0;

    RefleshRejectRate(iUseTimeMs);

    if (bGetLock)
    {
        m_bIsLockUsing = true;
    }
    m_oSerialLock.UnLock();

    return bGetLock;
}

//解锁
void WaitLock :: UnLock()
{
    m_oSerialLock.Lock();

    m_bIsLockUsing = false;
    m_oSerialLock.Interupt();

    m_oSerialLock.UnLock();
}

////////////////////////////////////////////
//获取部分值的参数
int WaitLock :: GetNowHoldThreadCount()
{
    return m_iWaitLockCount;
}

int WaitLock :: GetNowAvgThreadWaitTime()
{
    return m_iAvgLockUseTime;
}

int WaitLock :: GetNowRejectRate()
{
    return m_iRejectRate;
}

}


