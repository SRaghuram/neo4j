/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class LearnerContextImplTest
{
    @Test
    public void shouldOnlyLogLearnMissOnce()
    {
        // Given
        final AssertableLogProvider logProvider = new AssertableLogProvider();
        LearnerContextImpl ctx = new LearnerContextImpl( new InstanceId( 1 ), mock( CommonContextState.class ),
                logProvider, mock( Timeouts.class ), mock( PaxosInstanceStore.class ), mock( AcceptorInstanceStore.class ),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( HeartbeatContextImpl.class ) );

        // When
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1L ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1L ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2L ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2L ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1L ) );

        // Then
        logProvider.assertExactly(
                inLog( LearnerState.class ).warn( containsString( "Did not have learned value for Paxos instance 1." ) ),
                inLog( LearnerState.class ).warn( containsString( "Did not have learned value for Paxos instance 2." ) ),
                inLog( LearnerState.class ).warn( containsString( "Did not have learned value for Paxos instance 1." ) )
        );
    }

}
