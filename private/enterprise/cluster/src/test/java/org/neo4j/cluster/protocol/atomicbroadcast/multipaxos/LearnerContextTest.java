/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LearnerContextTest
{
    private final LogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldOnlyAllowHigherLastLearnedInstanceId()
    {
        // Given

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null, null,
                config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new InstanceId( 3 ) );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( 1L ) );
    }

    @Test
    public void shouldTrackLastKnownUpToDateAliveInstance()
    {
        // Given

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null, null,
                config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 3 ) );
        state.setLastKnownLearnedInstanceInCluster( 0, new InstanceId( 4 ) );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( 1L ) );
        assertThat( state.getLastKnownAliveUpToDateInstance(), equalTo( new InstanceId( 3 ) ));
    }

    @Test
    public void settingLastLearnedInstanceToNegativeOneShouldAlwaysWin()
    {
        // Given
        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext mpCtx = new MultiPaxosContext( null, Iterables.empty(),
                mock( ClusterConfiguration.class ), null, NullLogProvider.getInstance(),
                null, null, null, null,
                null, config );
        LearnerContext state = mpCtx.getLearnerContext();

        // When
        state.setLastKnownLearnedInstanceInCluster( 1, new InstanceId( 2 ) );
        state.setLastKnownLearnedInstanceInCluster( -1, null );

        // Then
        assertThat( state.getLastKnownLearnedInstanceInCluster(), equalTo( -1L ) );
        assertThat( state.getLastKnownAliveUpToDateInstance(), equalTo( new org.neo4j.cluster.InstanceId( 2 ) ));
    }
}
