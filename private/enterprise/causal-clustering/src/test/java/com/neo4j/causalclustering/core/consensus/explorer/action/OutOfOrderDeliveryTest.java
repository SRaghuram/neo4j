/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.explorer.action;

import com.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Election;
import com.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Heartbeat;
import com.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class OutOfOrderDeliveryTest
{
    @Test
    void shouldReOrder() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Election( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Heartbeat( member( 0 ) ) );

        // when
        ClusterState reOrdered = new OutOfOrderDelivery( member( 0 ) ).advance( clusterState );

        // then
        Assertions.assertEquals( new Heartbeat( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
        Assertions.assertEquals( new Election( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
    }
}
