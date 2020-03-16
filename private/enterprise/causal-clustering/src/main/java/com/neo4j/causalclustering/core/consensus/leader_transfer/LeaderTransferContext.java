package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

class LeaderTransferContext
{
    private final RaftId raftId;
    private final MemberId memberId;

    LeaderTransferContext( RaftId raftId, MemberId memberId )
    {
        this.raftId = raftId;
        this.memberId = memberId;
    }

    MemberId to()
    {
        return memberId;
    }

     RaftId raftId()
    {
        return raftId;
    }
}
