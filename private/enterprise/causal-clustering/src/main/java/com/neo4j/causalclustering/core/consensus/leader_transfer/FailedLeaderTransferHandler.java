package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

public interface FailedLeaderTransferHandler
{
    void handle( RaftMessages.LeadershipTransfer.Rejection rejection );
}
