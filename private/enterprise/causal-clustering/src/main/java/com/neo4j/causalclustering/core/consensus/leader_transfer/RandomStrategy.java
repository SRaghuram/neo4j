/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.util.List;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.StrategyUtils.selectRandom;

class RandomStrategy implements SelectionStrategy
{
    @Override
    public LeaderTransferTarget select( List<TransferCandidates> validTopologies )
    {
        return selectRandom( validTopologies )
                .flatMap( t -> selectRandom( t.members() ).map( m -> new LeaderTransferTarget( t.databaseId(), m ) ) )
                .orElse( LeaderTransferTarget.NO_TARGET );
    }
}
