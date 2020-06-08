/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.shuffle;

class RandomStrategy implements SelectionStrategy
{
    @Override
    public LeaderTransferTarget select( List<TransferCandidates> validTopologies )
    {
        if ( validTopologies.isEmpty() )
        {
            return LeaderTransferTarget.NO_TARGET;
        }

        var transferCandidates = randomTransferCandidates( validTopologies );
        var member = randomMember( transferCandidates.members() );
        return new LeaderTransferTarget( transferCandidates.databaseId(), member );
    }

    private TransferCandidates randomTransferCandidates( List<TransferCandidates> validTopologies )
    {
        var randomTopologyIdx = ThreadLocalRandom.current().nextInt( validTopologies.size() );
        return validTopologies.get( randomTopologyIdx );
    }

    private MemberId randomMember( Set<MemberId> members )
    {
        var membersList = new ArrayList<>( members );
        var randomMemberIdx = ThreadLocalRandom.current().nextInt( membersList.size() );
        return membersList.get( randomMemberIdx );
    }
}
