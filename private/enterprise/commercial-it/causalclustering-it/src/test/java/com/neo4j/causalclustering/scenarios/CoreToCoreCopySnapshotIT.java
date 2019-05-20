/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.test.causalclustering.ClusterRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.DbRepresentation;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_rotation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_flush_window_size;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

public class CoreToCoreCopySnapshotIT
{
    protected static final int NR_CORE_MEMBERS = 3;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( NR_CORE_MEMBERS )
            .withNumberOfReadReplicas( 0 );

    @Test
    public void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember source = DataCreator.createDataInOneTransaction( cluster, 1000 );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 5, TimeUnit.SECONDS );

        // shutdown the follower, remove the store, restart
        follower.shutdown();
        FileSystemAbstraction fs = clusterRule.testDirectory().getFileSystem();
        fs.deleteRecursively( follower.databaseLayout().databaseDirectory() );
        fs.deleteRecursively( follower.clusterStateDirectory() );
        follower.start();

        // then
        assertEquals( DbRepresentation.of( source.defaultDatabase() ), DbRepresentation.of( follower.defaultDatabase() ) );
    }

    @Test
    public void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "3 entries",
                CausalClusteringSettings.raft_log_rotation_size.name(), "1K" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        CoreClusterMember leader = DataCreator.createDataInOneTransaction( cluster, 10000 );

        // when
        for ( CoreClusterMember coreDb : cluster.coreMembers() )
        {
            coreDb.raftLogPruner().prune();
        }

        cluster.removeCoreMember( leader ); // to force a change of leader
        leader = cluster.awaitLeader();

        int newDbId = 3;
        cluster.addCoreMemberWithId( newDbId ).start();
        GraphDatabaseFacade newDb = cluster.getCoreMemberById( newDbId ).defaultDatabase();

        // then
        assertEquals( DbRepresentation.of( leader.defaultDatabase() ), DbRepresentation.of( newDb ) );
    }

    @Test
    public void shouldBeAbleToDownloadToRejoinedInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> coreParams = stringMap();
        coreParams.put( raft_log_rotation_size.name(), "1K" );
        coreParams.put( raft_log_pruning_strategy.name(), "keep_none" );
        coreParams.put( raft_log_pruning_frequency.name(), "100ms" );
        coreParams.put( state_machine_flush_window_size.name(), "64" );
        int numberOfTransactions = 100;

        // start the cluster
        Cluster cluster = clusterRule.withSharedCoreParams( coreParams ).startCluster();
        Timeout timeout = new Timeout( Clocks.systemClock(), 120, SECONDS );

        // accumulate some log files
        int firstServerLogFileCount;
        CoreClusterMember firstServer;
        do
        {
            timeout.assertNotTimedOut();
            firstServer = doSomeTransactions( cluster, numberOfTransactions );
            firstServerLogFileCount = getMostRecentLogIdOn( firstServer );
        }
        while ( firstServerLogFileCount < 5 );
        firstServer.shutdown();

        /* After shutdown we wait until we accumulate enough logs, and so that enough of the old ones
         * have been pruned, so that the rejoined instance won't be able to catch up to without a snapshot. */
        int oldestLogOnSecondServer;
        CoreClusterMember secondServer;
        do
        {
            timeout.assertNotTimedOut();
            secondServer = doSomeTransactions( cluster, numberOfTransactions );
            oldestLogOnSecondServer = getOldestLogIdOn( secondServer );
        }
        while ( oldestLogOnSecondServer < firstServerLogFileCount + 5 );

        // when
        firstServer.start();

        // then
        dataMatchesEventually( firstServer, List.of( secondServer ) );
    }

    private class Timeout
    {
        private final Clock clock;
        private final long absoluteTimeoutMillis;

        Timeout( Clock clock, long time, TimeUnit unit )
        {
            this.clock = clock;
            this.absoluteTimeoutMillis = clock.millis() + unit.toMillis( time );
        }

        void assertNotTimedOut()
        {
            if ( clock.millis() > absoluteTimeoutMillis )
            {
                throw new AssertionError( "Timed out" );
            }
        }
    }

    private int getOldestLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getLogFileNames().firstKey().intValue();
    }

    private int getMostRecentLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getLogFileNames().lastKey().intValue();
    }

    private CoreClusterMember doSomeTransactions( Cluster cluster, int count )
    {
        try
        {
            CoreClusterMember last = null;
            for ( int i = 0; i < count; i++ )
            {
                last = cluster.coreTx( ( db, tx ) ->
                {
                    Node node = db.createNode();
                    node.setProperty( "that's a bam", string( 1024 ) );
                    tx.success();
                } );
            }
            return last;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private String string( int numberOfCharacters )
    {
        StringBuilder s = new StringBuilder();
        for ( int i = 0; i < numberOfCharacters; i++ )
        {
            s.append( String.valueOf( i ) );
        }
        return s.toString();
    }

}
