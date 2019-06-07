/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.causalclustering.readreplica.CatchupPollingProcess;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.causalclustering.ClusterRule;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.DataCreator.NODE_PROPERTY_1;
import static com.neo4j.causalclustering.common.DataCreator.NODE_PROPERTY_1_PREFIX;
import static com.neo4j.causalclustering.common.DataCreator.createDataInOneTransaction;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReadReplicaReplicationIT
{
    private static final int NR_CORE_MEMBERS = 3;
    private static final int NR_READ_REPLICAS = 1;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( NR_CORE_MEMBERS )
            .withNumberOfReadReplicas( NR_READ_REPLICAS )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    @Test
    public void shouldNotBeAbleToWriteToReadReplica() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        GraphDatabaseFacade readReplica = cluster.findAnyReadReplica().defaultDatabase();

        // when
        try ( Transaction tx = readReplica.beginTx() )
        {
            Node node = readReplica.createNode();
            node.setProperty( NODE_PROPERTY_1, "baz_bat" );
            node.addLabel( DataCreator.LABEL );
            tx.success();
            fail( "should have thrown" );
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // then all good
        }
    }

    @Test
    public void allServersBecomeAvailable() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> readReplica.defaultDatabase().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllReadReplicas() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();
        int nodesBeforeReadReplicaStarts = 1;

        DataCreator.createSchema( cluster );

        // when
        for ( int i = 0; i < 100; i++ )
        {
            createDataInOneTransaction( cluster, nodesBeforeReadReplicaStarts );
        }

        Set<Path> labelScanStoreFiles = new HashSet<>();
        cluster.coreTx( ( db, tx ) -> gatherLabelScanStoreFiles( db, labelScanStoreFiles ) );

        AtomicBoolean labelScanStoreCorrectlyPlaced = new AtomicBoolean( false );
        Monitors monitors = new Monitors();
        ReadReplica rr = cluster.addReadReplicaWithIdAndMonitors( 0, monitors );
        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            if ( labelScanStoreFiles.contains( file.toPath().getFileName() ) )
            {
                labelScanStoreCorrectlyPlaced.set( true );
            }
        } );

        rr.start();

        for ( int i = 0; i < 100; i++ )
        {
            createDataInOneTransaction( cluster, nodesBeforeReadReplicaStarts );
        }

        // then
        for ( final ReadReplica server : cluster.readReplicas() )
        {
            GraphDatabaseService readReplica = server.defaultDatabase();
            try ( Transaction tx = readReplica.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( readReplica.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, is( 400L ) , 1, MINUTES );

                for ( Node node : readReplica.getAllNodes() )
                {
                    assertThat( node.getProperty( NODE_PROPERTY_1 ).toString(), startsWith( NODE_PROPERTY_1_PREFIX ) );
                }

                tx.success();
            }
        }

        assertTrue( labelScanStoreCorrectlyPlaced.get() );
    }

    private static void gatherLabelScanStoreFiles( GraphDatabaseAPI db, Set<Path> labelScanStoreFiles )
    {
        Path databaseDirectory = db.databaseLayout().databaseDirectory().toPath();
        LabelScanStore labelScanStore = db.getDependencyResolver().resolveDependency( LabelScanStore.class );
        try ( ResourceIterator<File> files = labelScanStore.snapshotStoreFiles() )
        {
            Path relativePath = databaseDirectory.relativize( files.next().toPath().toAbsolutePath() );
            labelScanStoreFiles.add( relativePath );
        }
    }

    @Test
    public void shouldShutdownRatherThanPullUpdatesFromCoreMemberWithDifferentStoreIdIfLocalStoreIsNonEmpty()
            throws Exception
    {
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        createDataInOneTransaction( cluster, 10 );

        cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 2, TimeUnit.SECONDS );

        // Get a read replica and make sure that it is operational
        ReadReplica readReplica = cluster.addReadReplicaWithId( 4 );
        readReplica.start();
        readReplica.defaultDatabase().beginTx().close();

        // Change the store id, so it should fail to join the cluster again
        changeStoreId( readReplica );
        readReplica.shutdown();

        readReplica.start();
        assertFailedToStart( readReplica, DEFAULT_DATABASE_NAME );
    }

    @Test
    public void aReadReplicShouldBeAbleToRejoinTheCluster() throws Exception
    {
        int readReplicaId = 4;
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        createDataInOneTransaction( cluster, 10 );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        // let's spend some time by adding more data
        createDataInOneTransaction( cluster, 10 );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );
        cluster.removeReadReplicaWithMemberId( readReplicaId );

        // let's spend some time by adding more data
        createDataInOneTransaction( cluster, 10 );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        Function<ClusterMember,DbRepresentation> toRep = db -> DbRepresentation.of( db.defaultDatabase() );
        Set<DbRepresentation> dbs = cluster.coreMembers().stream().map( toRep ).collect( toSet() );
        dbs.addAll( cluster.readReplicas().stream().map( toRep ).collect( toSet() ) );

        cluster.shutdown();

        assertEquals( 1, dbs.size() );
    }

    @Test
    public void readReplicasShouldRestartIfTheWholeClusterIsRestarted() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.shutdown();
        cluster.start();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> readReplica.defaultDatabase().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldBeAbleToDownloadANewStoreAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        createDataInOneTransaction( cluster, 10 );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        long highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

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
        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.defaultDatabase() ),
                equalTo( DbRepresentation.of( cluster.awaitLeader().defaultDatabase() ) ), 10, TimeUnit.SECONDS );
    }

    @Test
    public void shouldBeAbleToPullTxAfterHavingDownloadedANewStoreAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        createDataInOneTransaction( cluster, 10 );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        long highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

        readReplica.shutdown();

        CoreClusterMember core;
        do
        {
            core = createDataInOneTransaction( cluster, 1_000 );
        }
        while ( physicalLogFiles( core ).getLowestLogVersion() <= highestReadReplicaLogVersion );

        readReplica.start();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        // when
        createDataInOneTransaction( cluster, 10 );

        // then
        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.defaultDatabase() ),
                equalTo( DbRepresentation.of( cluster.awaitLeader().defaultDatabase() ) ), 10, TimeUnit.SECONDS );
    }

    @Test
    public void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        GraphDatabaseFacade readReplicaGraphDatabase = cluster.findAnyReadReplica().defaultDatabase();
        CatchupPollingProcess pollingClient = readReplicaGraphDatabase.getDependencyResolver()
                .resolveDependency( CatchupPollingProcess.class );
        pollingClient.stop();

        cluster.coreTx( ( coreGraphDatabase, transaction ) ->
        {
            coreGraphDatabase.createNode();
            transaction.success();
        } );

        GraphDatabaseFacade leaderDatabase = cluster.awaitLeader().defaultDatabase();
        long transactionVisibleOnLeader = transactionIdTracker( leaderDatabase ).newestEncounteredTxId();

        // when the poller is paused, transaction doesn't make it to the read replica
        try
        {
            transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 15 ) );
            fail( "should have thrown exception" );
        }
        catch ( TransactionFailureException e )
        {
            // expected timeout
        }

        // when the poller is resumed, it does make it to the read replica
        pollingClient.start();
        transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 15 ) );
    }

    private static TransactionIdTracker transactionIdTracker( GraphDatabaseAPI database )
    {
        Supplier<TransactionIdStore> transactionIdStore =
                database.getDependencyResolver().provideDependency( TransactionIdStore.class );
        AvailabilityGuard databaseAvailabilityGuard =
                database.getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class );
        return new TransactionIdTracker( transactionIdStore, databaseAvailabilityGuard, Clocks.nanoClock() );
    }

    private static LogFiles physicalLogFiles( ClusterMember clusterMember )
    {
        return clusterMember.defaultDatabase().getDependencyResolver().resolveDependency( LogFiles.class );
    }

    private static boolean readReplicasUpToDateAsTheLeader( CoreClusterMember leader, Collection<ReadReplica> readReplicas )
    {
        long leaderTxId = lastClosedTransactionId( true, leader.defaultDatabase() );
        return readReplicas.stream().map( ReadReplica::defaultDatabase )
                .map( db -> lastClosedTransactionId( false, db ) )
                .reduce( true, ( acc, txId ) -> acc && txId == leaderTxId, Boolean::logicalAnd );
    }

    private static void changeStoreId( ReadReplica replica ) throws IOException
    {
        File neoStoreFile = replica.databaseLayout().metadataStore();
        PageCache pageCache = replica.defaultDatabase().getDependencyResolver().resolveDependency( PageCache.class );
        MetaDataStore.setRecord( pageCache, neoStoreFile, TIME, System.currentTimeMillis() );
    }

    private static long lastClosedTransactionId( boolean fail, GraphDatabaseFacade db )
    {
        try
        {
            return db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                    .getLastClosedTransactionId();
        }
        catch ( IllegalStateException  | UnsatisfiedDependencyException /* db is shutdown or not available */ ex )
        {
            if ( !fail )
            {
                // the db is down we'll try again...
                return -1;
            }
            else
            {
                throw ex;
            }
        }
    }

    @Test
    public void shouldThrowExceptionIfReadReplicaRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).withRecordFormat( HighLimit.NAME ).startCluster();

        // when
        createDataInOneTransaction( cluster, 10 );

        String format = Standard.LATEST_NAME;
        ReadReplica readReplica = cluster.addReadReplicaWithIdAndRecordFormat( 0, format );
        readReplica.start();
        assertFailedToStart( readReplica, DEFAULT_DATABASE_NAME );
    }

    @Test
    public void shouldBeAbleToCopyStoresFromCoreToReadReplica() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CausalClusteringSettings.raft_log_rotation_size.name(), "1k",
                CausalClusteringSettings.raft_log_pruning_frequency.name(), "500ms",
                CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "1 entries" );
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).withSharedCoreParams( params )
                .withRecordFormat( HighLimit.NAME ).startCluster();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( Label.label( "L" ) );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
            }
            tx.success();
        } );

        long baseVersion = versionBy( cluster.awaitLeader().raftLogDirectory(), Math::max );

        CoreClusterMember coreGraphDatabase = null;
        for ( int j = 0; j < 2; j++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( Label.label( "L" ) );
                for ( int i = 0; i < 10; i++ )
                {
                    node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
                }
                tx.success();
            } );
        }

        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        assertEventually( "pruning happened", () -> versionBy( raftLogDir, Math::min ), greaterThan( baseVersion ), 5,
                SECONDS );

        // when
        cluster.addReadReplicaWithIdAndRecordFormat( 4, HighLimit.NAME ).start();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            assertEventually( "read replica available", () -> readReplica.defaultDatabase().isAvailable( 0 ), is( true ), 10,
                    SECONDS );
        }
    }

    private static long versionBy( File raftLogDir, BinaryOperator<Long> operator ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            SortedMap<Long,File> logs = new FileNames( raftLogDir ).getAllFiles( fileSystem, mock( Log.class ) );
            return logs.keySet().stream().reduce( operator ).orElseThrow( IllegalStateException::new );
        }
    }

    @Test
    public void pageFaultsFromReplicationMustCountInMetrics() throws Exception
    {
        // Given initial pin counts on all members
        Cluster cluster = clusterRule.startCluster();
        Function<ReadReplica,PageCacheCounters> getPageCacheCounters =
                ccm -> ccm.defaultDatabase().getDependencyResolver().resolveDependency( PageCacheCounters.class );
        List<PageCacheCounters> countersList =
                cluster.readReplicas().stream().map( getPageCacheCounters ).collect( Collectors.toList() );
        long[] initialPins = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();

        // when the leader commits a write transaction,
        DataCreator.createDataInOneTransaction( cluster, 1 );

        // then the replication should cause pins on a majority of core members to increase.
        // However, the commit returns as soon as the transaction has been replicated through the Raft log, which
        // happens before the transaction is applied on the members, and then replicated to read-replicas.
        // Therefor we are racing with the transaction application on the read-replicas, so we have to spin.
        int minimumUpdatedMembersCount = countersList.size() / 2 + 1;
        assertEventually( "Expected followers to eventually increase pin counts", () ->
        {
            long[] pinsAfterCommit = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();
            int membersWithIncreasedPinCount = 0;
            for ( int i = 0; i < initialPins.length; i++ )
            {
                long before = initialPins[i];
                long after = pinsAfterCommit[i];
                if ( before < after )
                {
                    membersWithIncreasedPinCount++;
                }
            }
            return membersWithIncreasedPinCount;
        }, Matchers.is( greaterThanOrEqualTo( minimumUpdatedMembersCount ) ), 10, SECONDS );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static void assertFailedToStart( ReadReplica readReplica, String databaseName )
    {
        ClusteredDatabaseContext defaultDatabase = readReplica
                .databaseManager()
                .getDatabaseContext( new TestDatabaseIdRepository().get( databaseName ) )
                .orElseThrow();

        assertTrue( defaultDatabase.isFailed() );
    }
}
