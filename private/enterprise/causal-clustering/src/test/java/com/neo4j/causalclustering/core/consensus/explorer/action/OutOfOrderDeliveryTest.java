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

import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class OutOfOrderDeliveryTest
{
    @Test
    void shouldReOrder() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( raftMember( 0 ) ) );
        clusterState.queues.get( raftMember( 0 ) ).add( new Election( raftMember( 0 ) ) );
        clusterState.queues.get( raftMember( 0 ) ).add( new Heartbeat( raftMember( 0 ) ) );

        // when
        ClusterState reOrdered = new OutOfOrderDelivery( raftMember( 0 ) ).advance( clusterState );

        // then
        Assertions.assertEquals( new Heartbeat( raftMember( 0 ) ), reOrdered.queues.get( raftMember( 0 ) ).poll() );
        Assertions.assertEquals( new Election( raftMember( 0 ) ), reOrdered.queues.get( raftMember( 0 ) ).poll() );
    }
}
