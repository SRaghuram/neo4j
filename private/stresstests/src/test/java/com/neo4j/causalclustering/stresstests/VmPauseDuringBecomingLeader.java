/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessageTimerResetMonitor;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.dbms.DatabaseStateChangedListener;
import com.neo4j.dbms.DbmsReconciler;
import com.neo4j.helper.Workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.MAXIMUM_DATABASE_NAME_LENGTH;
import static org.neo4j.configuration.helpers.DatabaseNameValidator.validateExternalDatabaseName;
import static org.neo4j.function.Predicates.await;

public class VmPauseDuringBecomingLeader extends Workload
{
    private final Cluster cluster;
    private final List<Long> resultTerms;
    private final int numberOfDatabases;
    private final Log log;

    private CoreClusterMember leaderToBeReTriggered;

    VmPauseDuringBecomingLeader( Control control, Resources resources, Config config )
    {
        super( control );

        this.cluster = resources.cluster();
        this.numberOfDatabases = config.numberOfDatabases();
        this.resultTerms = new ArrayList<>( numberOfDatabases );
        this.log = resources.logProvider().getLog( getClass() );
    }

    @Override
    public void prepare()
    {
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );

        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );

        cluster.coreMembers().forEach( core ->
                core.resolveDependency( SYSTEM_DATABASE_NAME, DbmsReconciler.class ).registerDatabaseStateChangedListener(
                        new VmPauseInstallerDatabaseStateChangedListener( core ) ) );
    }

    @Override
    protected void doWork() throws Exception
    {
        if ( resultTerms.size() >= numberOfDatabases )
        {
            control.onFinish();
            return;
        }
        leaderToBeReTriggered = null;

        var start = System.currentTimeMillis();
        var databaseName = createDatabaseWithRandomName();
        var resultTerm = -2L;

        try
        {
            // then wait for vm pause to happen
            try
            {
                await( () -> leaderToBeReTriggered, Objects::nonNull, 10, SECONDS );
            }
            catch ( TimeoutException te )
            {
                // if the installation of the monitor happens after the leader becomes leader the simulation of the pause is not possible, so the test
                // cannot be completed
                log.info( "Database %s gets ignored: simulation of vm pause could not be inserted or was not executed", databaseName );
                return;
            }

            var raftMachine = leaderToBeReTriggered.resolveDependency( databaseName, RaftMachine.class );
            // this blocks until LEADER becomes FOLLOWER if case is successful
            raftMachine.triggerElection();

            // in a few cases even triggering an election does not make the leader to step down, this means the simulation of the pause was no successful,
            // so the test cannot continue
            if ( raftMachine.isLeader() )
            {
                log.info( "Database %s gets ignored: simulation of vm pause was not successful - still leader after forced election trigger", databaseName );
                return;
            }

            var leader = cluster.awaitLeader( databaseName );
            var leaderLocator = leader.resolveDependency( databaseName, LeaderLocator.class );
            resultTerm = leaderLocator.getLeaderInfo().map( LeaderInfo::term ).orElse( -1L );
            log.info( "Database %s experienced the scenario, result term is %d", databaseName, resultTerm );
        }
        finally
        {
            // we need to wait this out otherwise the cluster won't shut down
            assertDatabaseEventuallyStarted( databaseName, cluster );
            resultTerms.add( resultTerm );
            log.info( "Started database %s in %d ms.", databaseName, System.currentTimeMillis() - start );
        }
    }

    @Override
    public void validate()
    {
        var allTestCount = resultTerms.size();
        var successfulTermCount = resultTerms.stream().filter( term -> term > 0 ).count();
        log.info( "Count of all/successful scenarios: %d/%d", allTestCount, successfulTermCount );

        // check that for databases which experienced the pause have a leader and its term is not 1
        resultTerms.stream().filter( term -> term != -2 ).forEach( term -> assertThat( term ).isGreaterThan( 1 ) );
    }

    private String createDatabaseWithRandomName() throws Exception
    {
        var databaseName = new NormalizedDatabaseName(
                random( 1, true, false ) +
                random( MAXIMUM_DATABASE_NAME_LENGTH - 1, true, true ) );

        validateExternalDatabaseName( databaseName );
        createDatabase( databaseName.name(), cluster );

        return databaseName.name();
    }

    private class VmPauseInstallerDatabaseStateChangedListener implements DatabaseStateChangedListener
    {
        private CoreClusterMember core;

        private VmPauseInstallerDatabaseStateChangedListener( CoreClusterMember core )
        {
            this.core = core;
        }

        @Override
        public void stateChange( DatabaseState previousState, DatabaseState newState )
        {
            try
            {
                if ( previousState.operatorState() == INITIAL && newState.operatorState() == STARTED && leaderToBeReTriggered == null )
                {
                    var databaseName = previousState.databaseId().name();
                    var monitors = core.resolveDependency( databaseName, RaftMonitors.class );
                    monitors.addMonitorListener( new VmPauseSimulatorMonitor( core, databaseName ) );
                    log.debug( "Monitor installed on %s %s", core.serverId(), databaseName );
                }
            }
            catch ( Exception t )
            {
                log.error( "Exception thrown during register pause installer", t );
            }
        }
    }

    private class VmPauseSimulatorMonitor implements RaftMessageTimerResetMonitor
    {
        private final CoreClusterMember core;
        private final String databaseName;
        private boolean first = true;

        private VmPauseSimulatorMonitor( CoreClusterMember core, String databaseName )
        {
            this.core = core;
            this.databaseName = databaseName;
        }

        @Override
        public void timerReset()
        {
            try
            {
                var raftMachine = core.resolveDependency( databaseName, RaftMachine.class );
                if ( first && raftMachine.currentRole() == Role.CANDIDATE && leaderToBeReTriggered == null )
                {
                    // The first time the raft machine calls RaftMessageTimerResetMonitor when moving from CANDIDATE we pause for a bit, allowing to the main
                    // thread to trigger an election, if would we wait long enough this would happen eventually too, but this allows to be more certain of it.
                    leaderToBeReTriggered = core;
                    log.debug( "Simulating VM Pause on %s %s", core.serverId(), databaseName );
                    SECONDS.sleep( 2 );
                    first = false;
                }
            }
            catch ( Throwable t )
            {
                log.error( "Exception thrown during pause", t );
            }
        }
    }
}
