package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.List;

public interface SelectionStrategy
{
    LeaderTransferContext select( List<DatabaseCoreTopology> validTopologies, MemberId myself );
}
