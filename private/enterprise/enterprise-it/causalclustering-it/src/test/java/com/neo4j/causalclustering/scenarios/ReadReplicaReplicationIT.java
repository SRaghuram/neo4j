/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupProcessManager;
import com.neo4j.causalclustering.readreplica.ReadReplicaDatabaseManager;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.bolt.txtracking.TransactionIdTrackerException;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.DataCreator.NODE_PROPERTY_1;
import static com.neo4j.causalclustering.common.DataCreator.NODE_PROPERTY_1_PREFIX;
import static com.neo4j.causalclustering.common.DataCreator.createDataInOneTransaction;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_topology_refresh;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@ClusterExtension
@TestInstance( PER_METHOD )
class ReadReplicaReplicationIT
{
    private static final int NR_CORE_MEMBERS = 3;
    private static final int NR_READ_REPLICAS = 1;

    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldNotBeAbleToWriteToReadReplica() throws Exception
    {
        // given
        var cluster = startClusterWithDefaultConfig();

        var readReplica = cluster.findAnyReadReplica().defaultDatabase();

        // when / then
        assertThrows( WriteOperationsNotAllowedException.class, () ->
        {
            try ( var tx = readReplica.beginTx() )
            {
                var node = tx.createNode();
                node.setProperty( NODE_PROPERTY_1, "baz_bat" );
                node.addLabel( DataCreator.LABEL );
                tx.commit();
            }
        } );
    }

    @Test
    void allServersBecomeAvailable() throws Exception
    {
        // given
        var cluster = startClusterWithDefaultConfig();

        // then
        for ( var readReplica : cluster.readReplicas() )
        {
            Callable<Boolean> availability = () -> readReplica.defaultDatabase().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, TRUE, 30, SECONDS );
        }
    }

    @Test
    void shouldEventuallyPullTransactionDownToAllReadReplicas() throws Exception
    {
        // given
        var cluster = startCluster( defaultClusterConfig().withNumberOfReadReplicas( 0 ) );
        var nodesBeforeReadReplicaStarts = 1;

        DataCreator.createSchema( cluster );

        // when
        for ( var i = 0; i < 100; i++ )
        {
            createDataInOneTransaction( cluster, nodesBeforeReadReplicaStarts );
        }

        Set<Path> labelScanStoreFiles = new HashSet<>();
        cluster.coreTx( ( db, tx ) -> gatherLabelScanStoreFiles( db, labelScanStoreFiles ) );

        var labelScanStoreCorrectlyPlaced = new AtomicBoolean( false );
        var monitors = new Monitors();
        var readReplica = cluster.addReadReplicaWithIdAndMonitors( 0, monitors );
        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            if ( labelScanStoreFiles.contains( file.getFileName() ) )
            {
                labelScanStoreCorrectlyPlaced.set( true );
            }
        } );
        readReplica.start();

        for ( var i = 0; i < 100; i++ )
        {
            createDataInOneTransaction( cluster, nodesBeforeReadReplicaStarts );
        }

        // then
        for ( var server : cluster.readReplicas() )
        {
            var readReplicaDb = server.defaultDatabase();
            try ( var tx = readReplicaDb.beginTx() )
            {
                Callable<Long> nodeCount = () -> count( tx.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, equalityCondition( 400L ), 1, MINUTES );

                for ( var node : tx.getAllNodes() )
                {
                    assertThat( node.getProperty( NODE_PROPERTY_1 ).toString(), startsWith( NODE_PROPERTY_1_PREFIX ) );
                }

                tx.commit();
            }
        }

        assertTrue( labelScanStoreCorrectlyPlaced.get() );
    }

    private static void gatherLabelScanStoreFiles( GraphDatabaseAPI db, Set<Path> labelScanStoreFiles )
    {
        var databaseDirectory = db.databaseLayout().databaseDirectory();
        var labelScanStore = db.getDependencyResolver().resolveDependency( LabelScanStore.class );
        try ( var files = labelScanStore.snapshotStoreFiles() )
        {
            var relativePath = databaseDirectory.relativize( files.next().toAbsolutePath() );
            labelScanStoreFiles.add( relativePath );
        }
    }

    @Test
    void shouldShutdownRatherThanPullUpdatesFromCoreMemberWithDifferentStoreIdIfLocalStoreIsNonEmpty()
            throws Exception
    {
        var cluster = startCluster( defaultClusterConfig().withNumberOfReadReplicas( 0 ) );

        createDataInOneTransaction( cluster, 10 );

        assertNotNull( cluster.getMemberWithAnyRole( Role.FOLLOWER ) );

        // Get a read replica and make sure that it is operational
        var readReplica = cluster.addReadReplicaWithId( 4 );
        readReplica.start();
        readReplica.defaultDatabase().beginTx().close();

        // Change the store id, so it should fail to join the cluster again
        changeStoreId( readReplica );
        readReplica.shutdown();

        readReplica.start();
        assertFailedToStart( readReplica, DEFAULT_DATABASE_NAME );
    }

    @Test
    void aReadReplicaShouldBeAbleToRejoinTheCluster() throws Exception
    {
        var readReplicaId = 4;
        var cluster = startCluster( defaultClusterConfig().withNumberOfReadReplicas( 0 ) );

        createDataInOneTransaction( cluster, 10 );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        // let's spend some time by adding more data
        createDataInOneTransaction( cluster, 10 );

        assertReadReplicasEventuallyUpToDateWithLeader( cluster );
        cluster.removeReadReplicaWithMemberId( readReplicaId );

        // let's spend some time by adding more data
        var lastMember = createDataInOneTransaction( cluster, 10 );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        assertReadReplicasEventuallyUpToDateWithLeader( cluster );

        dataMatchesEventually( lastMember, cluster.allMembers() );

        cluster.shutdown();
    }

    @Test
    void readReplicasShouldRestartIfTheWholeClusterIsRestarted() throws Exception
    {
        // given
        var cluster = startClusterWithDefaultConfig();

        // when
        cluster.shutdown();
        cluster.start();

        // then
        for ( var readReplica : cluster.readReplicas() )
        {
            Callable<Boolean> availability = () -> readReplica.defaultDatabase().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, TRUE, 30, SECONDS );
        }
    }

    @Test
    void shouldBeAbleToDownloadANewStoreAfterPruning() throws Exception
    {
        // given
        var params = Map.of( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        var cluster = startCluster( defaultClusterConfig().withSharedCoreParams( params ) );

        createDataInOneTransaction( cluster, 10 );

        assertReadReplicasEventuallyUpToDateWithLeader( cluster );

        var readReplica = cluster.getReadReplicaById( 0 );
        var highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

        // when
        readReplica.shutdown();

        CoreClusterMember core;
        do
        {
            core = createDataInOneTransaction( cluster, 1_000 );
        }
        while ( physicalLogFiles( core ).getLowestLogVersion() <= highestReadReplicaLogVersion );

        readReplica.start();

        // then
        assertReadReplicasEventuallyUpToDateWithLeader( cluster );

        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.defaultDatabase() ),
                equalityCondition( DbRepresentation.of( cluster.awaitLeader().defaultDatabase() ) ), 30, SECONDS );
    }

    @Test
    void shouldBeAbleToPullTxAfterHavingDownloadedANewStoreAfterPruning() throws Exception
    {
        // given
        var params = Map.of( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        var cluster = startCluster( defaultClusterConfig().withSharedCoreParams( params ) );

        createDataInOneTransaction( cluster, 10 );

        assertReadReplicasEventuallyUpToDateWithLeader( cluster );

        var readReplica = cluster.getReadReplicaById( 0 );
        var highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

        readReplica.shutdown();

        CoreClusterMember core;
        do
        {
            core = createDataInOneTransaction( cluster, 1_000 );
        }
        while ( physicalLogFiles( core ).getLowestLogVersion() <= highestReadReplicaLogVersion );

        readReplica.start();

        // when
        createDataInOneTransaction( cluster, 10 );
        assertReadReplicasEventuallyUpToDateWithLeader( cluster );

        // then
        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.defaultDatabase() ),
                equalityCondition( DbRepresentation.of( cluster.awaitLeader().defaultDatabase() ) ), 30, SECONDS );
    }

    @Test
    void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        var cluster = startClusterWithDefaultConfig();

        var readReplica = cluster.findAnyReadReplica();
        var readReplicaGraphDatabase = readReplica.defaultDatabase();
        var catchupProcessManager = readReplicaGraphDatabase.getDependencyResolver().resolveDependency( CatchupProcessManager.class );
        catchupProcessManager.stop();

        cluster.coreTx( ( coreGraphDatabase, transaction ) ->
        {
            transaction.createNode();
            transaction.commit();
        } );

        var leader = cluster.awaitLeader();
        var databaseId = defaultDatabaseId( leader );
        var transactionVisibleOnLeader = transactionIdTracker( leader ).newestTransactionId( databaseId );

        // when the poller is paused, transaction doesn't make it to the read replica
        assertThrows( TransactionIdTrackerException.class,
                () -> transactionIdTracker( readReplica ).awaitUpToDate( databaseId, transactionVisibleOnLeader, ofSeconds( 30 ) ) );

        // when the poller is resumed, it does make it to the read replica
        catchupProcessManager.start();
        transactionIdTracker( readReplica ).awaitUpToDate( databaseId, transactionVisibleOnLeader, ofSeconds( 30 ) );
    }

    @Test
    void transactionsShouldNotAppearOnTheReadReplicaWhileCatchupPollingIsPaused() throws Throwable
    {
        // given
        var cluster = startClusterWithDefaultConfig();

        var readReplica = cluster.findAnyReadReplica();
        var readReplicaGraphDatabase = readReplica.defaultDatabase();
        var catchupProcessManager = readReplicaGraphDatabase.getDependencyResolver().resolveDependency( CatchupProcessManager.class );
        catchupProcessManager.pauseCatchupProcess();

        var leader = cluster.coreTx( ( coreGraphDatabase, transaction ) ->
                                                              {
                                                                  transaction.createNode();
                                                                  transaction.commit();
                                                              } );

        var databaseId = defaultDatabaseId( leader );
        var transactionVisibleOnLeader = transactionIdTracker( leader ).newestTransactionId( databaseId );

        // when the poller is paused, transaction doesn't make it to the read replica
        assertThrows( TransactionIdTrackerException.class,
                      () -> transactionIdTracker( readReplica ).awaitUpToDate( databaseId, transactionVisibleOnLeader, ofSeconds( 30 ) ) );

        // when the poller is resumed, it does make it to the read replica
        catchupProcessManager.resumeCatchupProcess();
        transactionIdTracker( readReplica ).awaitUpToDate( databaseId, transactionVisibleOnLeader, ofSeconds( 30 ) );
    }

    private static TransactionIdTracker transactionIdTracker( ClusterMember member )
    {
        var reconciledTxTracker = member.systemDatabase().getDependencyResolver().resolveDependency( ReconciledTransactionTracker.class );
        return new TransactionIdTracker( member.managementService(), reconciledTxTracker, new Monitors(), Clocks.nanoClock() );
    }

    private static NamedDatabaseId defaultDatabaseId( ClusterMember member )
    {
        var dbApi = (GraphDatabaseAPI) member.managementService().database( DEFAULT_DATABASE_NAME );
        return dbApi.getDependencyResolver().resolveDependency( Database.class ).getNamedDatabaseId();
    }

    private static LogFiles physicalLogFiles( ClusterMember clusterMember )
    {
        return clusterMember.defaultDatabase().getDependencyResolver().resolveDependency( LogFiles.class );
    }

    private static void changeStoreId( ReadReplica replica ) throws IOException
    {
        var neoStoreFile = replica.databaseLayout().metadataStore();
        var pageCache = replica.defaultDatabase().getDependencyResolver().resolveDependency( PageCache.class );
        MetaDataStore.setRecord( pageCache, neoStoreFile, TIME, System.currentTimeMillis(), NULL );
    }

    @Test
    void shouldThrowExceptionIfReadReplicaRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        var cluster = startCluster( defaultClusterConfig().withNumberOfReadReplicas( 0 ).withRecordFormat( HighLimit.NAME ) );

        // when
        createDataInOneTransaction( cluster, 10 );

        var format = Standard.LATEST_NAME;
        var readReplica = cluster.addReadReplicaWithIdAndRecordFormat( 0, format );
        readReplica.start();
        assertFailedToStart( readReplica, DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldBeAbleToCopyStoresFromCoreToReadReplica() throws Exception
    {
        // given
        var params = Map.of( CausalClusteringSettings.raft_log_rotation_size.name(), "1k",
                CausalClusteringSettings.raft_log_pruning_frequency.name(), "500ms",
                CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "1 entries" );

        var clusterConfig = defaultClusterConfig()
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParams( params )
                .withRecordFormat( HighLimit.NAME );

        var cluster = startCluster( clusterConfig );

        cluster.coreTx( ( db, tx ) ->
        {
            var node = tx.createNode( Label.label( "L" ) );
            for ( var i = 0; i < 10; i++ )
            {
                node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
            }
            tx.commit();
        } );

        var baseVersion = versionBy( cluster.awaitLeader().raftLogDirectory( DEFAULT_DATABASE_NAME ), Math::max );

        CoreClusterMember coreGraphDatabase = null;
        for ( var j = 0; j < 2; j++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) ->
            {
                var node = tx.createNode( Label.label( "L" ) );
                for ( var i = 0; i < 10; i++ )
                {
                    node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
                }
                tx.commit();
            } );
        }

        var raftLogDir = coreGraphDatabase.raftLogDirectory( DEFAULT_DATABASE_NAME );
        assertEventually( "pruning happened", () -> versionBy( raftLogDir, Math::min ), v -> v > baseVersion, 30, SECONDS );

        // when
        cluster.addReadReplicaWithIdAndRecordFormat( 4, HighLimit.NAME ).start();

        // then
        for ( var readReplica : cluster.readReplicas() )
        {
            assertEventually( "read replica available", () -> readReplica.defaultDatabase().isAvailable( 0 ), TRUE, 30, SECONDS );
        }
    }

    private static long versionBy( File raftLogDir, BinaryOperator<Long> operator ) throws IOException
    {
        try ( var fileSystem = new DefaultFileSystemAbstraction() )
        {
            var logs = new FileNames( raftLogDir ).getAllFiles( fileSystem, mock( Log.class ) );
            return logs.keySet().stream().reduce( operator ).orElseThrow( IllegalStateException::new );
        }
    }

    @Test
    void pageFaultsFromReplicationMustCountInMetrics() throws Exception
    {
        // Given initial pin counts on all members
        var cluster = startClusterWithDefaultConfig();
        Function<ReadReplica,PageCacheCounters> getPageCacheCounters =
                ccm -> ccm.defaultDatabase().getDependencyResolver().resolveDependency( PageCacheCounters.class );
        var countersList = cluster.readReplicas().stream().map( getPageCacheCounters ).collect( Collectors.toList() );
        var initialPins = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();

        // when the leader commits a write transaction,
        DataCreator.createDataInOneTransaction( cluster, 1 );

        // then the replication should cause pins on a majority of core members to increase.
        // However, the commit returns as soon as the transaction has been replicated through the Raft log, which
        // happens before the transaction is applied on the members, and then replicated to read-replicas.
        // Therefor we are racing with the transaction application on the read-replicas, so we have to spin.
        var minimumUpdatedMembersCount = countersList.size() / 2 + 1;
        assertEventually( "Expected followers to eventually increase pin counts", () ->
        {
            var pinsAfterCommit = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();
            var membersWithIncreasedPinCount = 0;
            for ( var i = 0; i < initialPins.length; i++ )
            {
                var before = initialPins[i];
                var after = pinsAfterCommit[i];
                if ( before < after )
                {
                    membersWithIncreasedPinCount++;
                }
            }
            return membersWithIncreasedPinCount;
        }, v -> v >= minimumUpdatedMembersCount, 30, SECONDS );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static void assertFailedToStart( ReadReplica readReplica, String databaseName )
    {
        ReadReplicaDatabaseManager databaseManager = readReplica.resolveDependency( SYSTEM_DATABASE_NAME, ReadReplicaDatabaseManager.class );
        var dbStateService = readReplica.resolveDependency( SYSTEM_DATABASE_NAME, DatabaseStateService.class );
        var db = databaseManager.getDatabaseContext( databaseName ).orElseThrow();
        assertTrue( dbStateService.causeOfFailure( db.databaseId() ).isPresent() );
    }

    private Cluster startClusterWithDefaultConfig() throws Exception
    {
        return startCluster( defaultClusterConfig() );
    }

    private Cluster startCluster( ClusterConfig clusterConfig ) throws Exception
    {
        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }

    private static ClusterConfig defaultClusterConfig()
    {
        return clusterConfig()
                .withNumberOfCoreMembers( NR_CORE_MEMBERS )
                .withNumberOfReadReplicas( NR_READ_REPLICAS )
                .withSharedCoreParam( cluster_topology_refresh, "5s" )
                .withSharedReadReplicaParam( cluster_topology_refresh, "5s" );
    }

    private static void assertReadReplicasEventuallyUpToDateWithLeader( Cluster cluster )
    {
        assertEventually( () -> ReadReplicasProgress.of( cluster ), new HamcrestCondition<>( readReplicasUpToDateWithLeader() ), 1, MINUTES );
    }

    private static long lastClosedTransactionId( boolean fail, GraphDatabaseFacade db )
    {
        try
        {
            return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
        }
        catch ( Exception e )
        {
            if ( !fail )
            {
                // the db is down we'll try again...
                return -1;
            }
            throw e;
        }
    }

    private static Matcher<ReadReplicasProgress> readReplicasUpToDateWithLeader()
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            protected boolean matchesSafely( ReadReplicasProgress progress )
            {
                return progress.readReplicasUpToDateWithLeader();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Read replicas up-to-date with the leader" );
            }

            @Override
            protected void describeMismatchSafely( ReadReplicasProgress progress, Description description )
            {
                description.appendText( "Leader's last committed transaction ID: " ).appendValue( progress.leaderLastClosedTxId ).appendText( "\n" )
                        .appendText( "Read replicas last committed transaction IDs: " ).appendValue( progress.readReplicaLastClosedTxIds );
            }
        };
    }

    private static class ReadReplicasProgress
    {
        final long leaderLastClosedTxId;
        final Map<ReadReplica,Long> readReplicaLastClosedTxIds;

        ReadReplicasProgress( long leaderLastClosedTxId, Map<ReadReplica,Long> readReplicaLastClosedTxIds )
        {
            this.leaderLastClosedTxId = leaderLastClosedTxId;
            this.readReplicaLastClosedTxIds = readReplicaLastClosedTxIds;
        }

        static ReadReplicasProgress of( Cluster cluster ) throws TimeoutException
        {
            var leader = cluster.awaitLeader();
            var leaderLastClosedTxId = lastClosedTransactionId( true, leader.defaultDatabase() );

            var readReplicaLastClosedTxIds = cluster.readReplicas()
                    .stream()
                    .collect( toMap( identity(), readReplica -> lastClosedTransactionId( false, readReplica.defaultDatabase() ) ) );

            return new ReadReplicasProgress( leaderLastClosedTxId, readReplicaLastClosedTxIds );
        }

        boolean readReplicasUpToDateWithLeader()
        {
            return readReplicaLastClosedTxIds.values()
                    .stream()
                    .allMatch( readReplicaLastClosedTxId -> readReplicaLastClosedTxId == leaderLastClosedTxId );
        }
    }
}
