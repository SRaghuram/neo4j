/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.explorer.action;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Election;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Heartbeat;
import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class OutOfOrderDeliveryTest
{
    @Test
    public void shouldReOrder() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Election( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Heartbeat( member( 0 ) ) );

        // when
        ClusterState reOrdered = new OutOfOrderDelivery( member( 0 ) ).advance( clusterState );

        // then
        assertEquals( new Heartbeat( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
        assertEquals( new Election( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
    }
}
