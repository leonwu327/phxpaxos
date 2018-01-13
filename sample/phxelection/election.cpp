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

#include "election.h"
#include <assert.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

using namespace phxpaxos;
using namespace std;

void custLog(const int iLogLevel, const char * pcFormat, va_list args) {
    char sBuf[1024] = {0};
    vsnprintf(sBuf, sizeof(sBuf), pcFormat, args);
    std::cout << sBuf << std::endl;
}

namespace phxelection
{

PhxElection :: PhxElection(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList)
    : m_oMyNode(oMyNode), m_vecNodeList(vecNodeList), m_poPaxosNode(nullptr)
{
}

PhxElection :: ~PhxElection()
{
    delete m_poPaxosNode;
}

int PhxElection :: MakeLogStoragePath(std::string & sLogStoragePath)
{
    char sTmp[128] = {0};
    snprintf(sTmp, sizeof(sTmp), "./logpath_%s_%d", m_oMyNode.GetIP().c_str(), m_oMyNode.GetPort());

    sLogStoragePath = string(sTmp);

    if (access(sLogStoragePath.c_str(), F_OK) == -1)
    {
        if (mkdir(sLogStoragePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
        {       
            printf("Create dir fail, path %s\n", sLogStoragePath.c_str());
            return -1;
        }       
    }

    return 0;
}

void PhxElection :: OnMasterChange(const int iGroupIdx, const NodeInfo & oNewMaster, const uint64_t llVersion)
{
    printf("master change!!! groupidx %d newmaster ip %s port %d version %lu\n",
        iGroupIdx, oNewMaster.GetIP().c_str(), oNewMaster.GetPort(), llVersion);
}

int PhxElection :: RunPaxos()
{
    //选项类，设置所有用户可见的可变参数
    Options oOptions;

    //创建日志路径, 至于是什么日志，先不关心
    int ret = MakeLogStoragePath(oOptions.sLogStoragePath);
    if (ret != 0)
    {
        return ret;
    }
    //add by leonwu 调试日志
    oOptions.pLogFunc = custLog;

    //group的数量，group只是为了并发，每个group之间并没有交集
    oOptions.iGroupCount = 1;

    //设置本机节点
    oOptions.oMyNode = m_oMyNode;

    //设置集群的节点列表
    oOptions.vecNodeInfoList = m_vecNodeList;

    //创建状态机
    //open inside master state machine
    GroupSMInfo oSMInfo;

    //状态机的组id
    oSMInfo.iGroupIdx = 0;

    //是否支持选主的操作
    oSMInfo.bIsUseMaster = true;

    //将状态机放到状态机列表中
    oOptions.vecGroupSMInfoList.push_back(oSMInfo);

    //设置状态机改变之后支持回调
    oOptions.bOpenChangeValueBeforePropose = true;

    //主机改变之后的回调函数
    oOptions.pMasterChangeCallback = PhxElection::OnMasterChange;

    ret = Node::RunNode(oOptions, m_poPaxosNode);
    if (ret != 0)
    {
        printf("run paxos fail, ret %d\n", ret);
        return ret;
    }

    //设置租约时长
    //you can change master lease in real-time.
    m_poPaxosNode->SetMasterLease(0, 3000);

    printf("run paxos ok\n");
    return 0;
}

const phxpaxos::NodeInfo PhxElection :: GetMaster()
{
    //获取组的master
    //only one group, so groupidx is 0.
    return m_poPaxosNode->GetMaster(0);
}

const phxpaxos::NodeInfo PhxElection :: GetMasterWithVersion(uint64_t & llVersion)
{
    return m_poPaxosNode->GetMasterWithVersion(0, llVersion);
}

const bool PhxElection :: IsIMMaster()
{
    return m_poPaxosNode->IsIMMaster(0);
}

}


