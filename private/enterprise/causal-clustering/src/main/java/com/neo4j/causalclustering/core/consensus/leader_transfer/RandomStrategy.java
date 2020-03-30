/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.shuffle;

class RandomStrategy implements SelectionStrategy
{
    @Override
    public LeaderTransferContext select( List<TransferCandidates> validTopologies )
    {
        var databaseCoreTopologies = new ArrayList<>( validTopologies );
        shuffle( databaseCoreTopologies );
        for ( TransferCandidates validTopology : databaseCoreTopologies )
        {
            var members = new ArrayList<>( validTopology.members() );
            shuffle( members );
            if ( !members.isEmpty() )
            {
                return new LeaderTransferContext( validTopology.raftId(), members.get( 0 ) );
            }
        }
        return LeaderTransferContext.NO_TARGET;
    }
}
