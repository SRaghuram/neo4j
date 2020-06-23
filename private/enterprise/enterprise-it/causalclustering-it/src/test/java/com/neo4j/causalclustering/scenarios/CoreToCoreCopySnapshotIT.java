/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.state.RaftLogPruner;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_frequency;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_pruning_strategy;
import static com.neo4j.configuration.CausalClusteringSettings.raft_log_rotation_size;
import static com.neo4j.configuration.CausalClusteringSettings.state_machine_flush_window_size;
import static com.neo4j.configuration.CausalClusteringSettings.store_copy_chunk_size;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

@ClusterExtension
@ExtendWith( DefaultFileSystemExtension.class )
@TestInstance( PER_METHOD )
class CoreToCoreCopySnapshotIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private FileSystemAbstraction fs;

    @Test
    void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = startCluster( emptyMap() );

        CoreClusterMember source = DataCreator.createDataInOneTransaction( cluster, 1000 );

        // when
        CoreClusterMember follower = cluster.getMemberWithAnyRole( Role.FOLLOWER );

        // shutdown the follower, remove the store, restart
        follower.shutdown();
        fs.deleteRecursively( follower.databaseLayout().databaseDirectory() );
        fs.deleteRecursively( follower.clusterStateDirectory() );
        follower.start();

        // then
        assertEquals( DbRepresentation.of( source.defaultDatabase() ), DbRepresentation.of( follower.defaultDatabase() ) );
    }

    @Test
    void shouldSupportDifferentChunkSizesInStoreCopy() throws Exception
    {
        // given
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withInstanceCoreParam( store_copy_chunk_size, i -> String.valueOf( (i + 1) * 4096 ) );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        CoreClusterMember source = DataCreator.createDataInOneTransaction( cluster, 1000 );

        // when
        CoreClusterMember follower = cluster.getMemberWithAnyRole( Role.FOLLOWER );

        // shutdown the follower, remove the store, restart
        follower.shutdown();
        fs.deleteRecursively( follower.databaseLayout().databaseDirectory() );
        fs.deleteRecursively( follower.clusterStateDirectory() );
        follower.start();

        // then
        assertEquals( DbRepresentation.of( source.defaultDatabase() ), DbRepresentation.of( follower.defaultDatabase() ) );
    }

    @Test
    void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "3 entries",
                CausalClusteringSettings.raft_log_rotation_size.name(), "1K" );

        Cluster cluster = startCluster( params );

        CoreClusterMember leader = DataCreator.createDataInOneTransaction( cluster, 10000 );

        // when
        for ( CoreClusterMember coreDb : cluster.coreMembers() )
        {
            coreDb.resolveDependency( DEFAULT_DATABASE_NAME, RaftLogPruner.class ).prune();
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
    void shouldBeAbleToDownloadToRejoinedInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> coreParams = stringMap();
        coreParams.put( raft_log_rotation_size.name(), "1K" );
        coreParams.put( raft_log_pruning_strategy.name(), "keep_none" );
        coreParams.put( raft_log_pruning_frequency.name(), "100ms" );
        coreParams.put( state_machine_flush_window_size.name(), "64" );
        int numberOfTransactions = 10;

        // start the cluster
        Cluster cluster = startCluster( coreParams );
        Timeout timeout = new Timeout( Clocks.systemClock(), 120, SECONDS );

        // accumulate some log files
        int firstServerLogFileCount;
        CoreClusterMember firstServer;
        do
        {
            timeout.assertNotTimedOut();
            firstServer = doSomeTransactions( cluster, numberOfTransactions );
            Thread.sleep( 1000 );
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
            Thread.sleep( 1000 );
            oldestLogOnSecondServer = getOldestLogIdOn( secondServer );
        }
        while ( oldestLogOnSecondServer < firstServerLogFileCount + 5 );

        // when
        firstServer.start();

        // then
        dataMatchesEventually( firstServer, List.of( secondServer ) );
    }

    private Cluster startCluster( Map<String,String> params ) throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParams( params );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }

    private static class Timeout
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

    private static int getOldestLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getRaftLogFileNames( DEFAULT_DATABASE_NAME ).firstKey().intValue();
    }

    private static int getMostRecentLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getRaftLogFileNames( DEFAULT_DATABASE_NAME ).lastKey().intValue();
    }

    private static CoreClusterMember doSomeTransactions( Cluster cluster, int count )
    {
        try
        {
            CoreClusterMember last = null;
            for ( int i = 0; i < count; i++ )
            {
                last = cluster.coreTx( ( db, tx ) ->
                {
                    Node node = tx.createNode();
                    node.setProperty( "that's a bam", randomAlphanumeric( 1024 ) );
                    tx.commit();
                } );
            }
            return last;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
