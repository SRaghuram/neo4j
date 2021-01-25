/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.DatabaseStateChangedListener;
import com.neo4j.dbms.DbmsReconciler;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.function.Predicates.await;

@ClusterExtension
class ClusterStartWithVMPauseIT
{
    private static final String DATABASE_NAME_PREFIX = "foo";
    private static final int MAX_TRIES = 10;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private volatile CoreClusterMember leader;

    private int index;

    @Test
    void followersWithNotStartedTimersShouldReactToPreElection() throws Exception
    {
        var config = ClusterConfig.clusterConfig()
                .withSharedCoreParam( CausalClusteringSettings.leader_balancing, CausalClusteringSettings.SelectionStrategies.NO_BALANCING.name() )
                .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "15s-16s" )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( config );
        cluster.start();
        cluster.coreMembers().forEach( core ->
                core.resolveDependency( SYSTEM_DATABASE_NAME, DbmsReconciler.class ).registerDatabaseStateChangedListener(
                        new VmPauseInstallerDatabaseStateChangedListener( core ) ) );

        var scenarioReproduced = false;

        while ( index < MAX_TRIES && !scenarioReproduced )
        {
            scenarioReproduced = reproduce();
            index++;
        }

        assumeTrue( scenarioReproduced, format( "Test scenario cannot be reproduced in %d rounds, ignoring test", MAX_TRIES ) );
    }

    private boolean reproduce() throws Exception
    {
        // given
        leader = null;
        var databaseName = format( "%s%02d", DATABASE_NAME_PREFIX, index );

        // when
        createDatabase( databaseName, cluster );

        try
        {
            // then wait for vm pause to happen
            try
            {
                await( () -> leader, Objects::nonNull, 10, SECONDS );
            }
            catch ( TimeoutException te )
            {
                // if the installation of the monitor happens after the leader becomes leader the simulation
                // of the pause is not possible, so the test cannot be completed
                return false;
            }

            var raftMachine = leader.resolveDependency( databaseName, RaftMachine.class );
            // this blocks until LEADER becomes FOLLOWER if case is successful
            raftMachine.triggerElection();

            // in a few cases even triggering an election does not make the leader to step down, this means the simulation of the pause was no successful,
            // so the test cannot continue
            if ( raftMachine.currentRole() != Role.FOLLOWER )
            {
                return false;
            }

            // wait until a leader becomes leader again this time with term 2
            var leader = cluster.awaitLeader( databaseName );
            var leaderLocator = leader.resolveDependency( databaseName, LeaderLocator.class );
            assertThat( leaderLocator.getLeaderInfo().map( LeaderInfo::term ).orElse( -1L ) ).isGreaterThan( 1 );

            return true;
        }
        finally
        {
            // we need to wait this out otherwise the cluster won't shut down
            assertDatabaseEventuallyStarted( databaseName, cluster );
        }
    }

    private class VmPauseInstallerDatabaseStateChangedListener implements DatabaseStateChangedListener
    {
        private final CoreClusterMember core;

        private VmPauseInstallerDatabaseStateChangedListener( CoreClusterMember core )
        {
            this.core = core;
        }

        @Override
        public void stateChange( DatabaseState previousState, DatabaseState newState )
        {
            try
            {
                if ( leader == null && previousState.operatorState() == INITIAL && newState.operatorState() == STARTED )
                {
                    var databaseName = previousState.databaseId().name();
                    var raftMachine = core.resolveDependency( databaseName, RaftMachine.class );
                    var raftMemberId = raftMachine.memberId();
                    var leaderListener = new VmPauseSimulatorMonitor( core, raftMemberId );
                    raftMachine.registerListener( leaderListener );
                }
            }
            catch ( Exception t )
            {
                fail( "Exception thrown during register pause installer", t );
            }
        }
    }

    private class VmPauseSimulatorMonitor implements LeaderListener
    {
        private final CoreClusterMember core;
        private final RaftMemberId raftMemberId;

        VmPauseSimulatorMonitor( CoreClusterMember core, RaftMemberId raftMemberId )
        {
            this.core = core;
            this.raftMemberId = raftMemberId;
        }

        @Override
        public void onLeaderSwitch( LeaderInfo leaderInfo )
        {
            try
            {
                if ( leader == null && raftMemberId.equals( leaderInfo.memberId() ) )
                {
                    // The first time the raft machine calls LeaderListener when moving from CANDIDATE to LEADER we pause for a bit, allowing to the main
                    // thread to trigger an election, if would we wait long enough this would happen eventually too, but this allows to be more certain of it.
                    leader = core;
                    SECONDS.sleep( 2 );
                }
            }
            catch ( Throwable t )
            {
                fail( "Exception thrown during pause", t );
            }
        }
    }
}
