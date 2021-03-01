/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static java.lang.Integer.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class TransactionLogUpgradeIT
{

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private FileSystemAbstraction fs;

    private Cluster cluster;
    private GraphDatabaseService systemDb;

    @BeforeEach
    void before() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                                                   .withNumberOfCoreMembers( 2 )
                                                   .withNumberOfReadReplicas( 1 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        var systemLeader = cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        systemDb = systemLeader.managementService().database( SYSTEM_DATABASE_NAME );
    }

    @AfterEach
    void after()
    {
        cluster.shutdown();
    }

    @Test
    void testBasicVersionLifecycle()
    {
        // the system DB will be initialised with the default version for this binary
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );

        // BTW this should never be manipulated directly outside tests
        setRuntimeVersion( DbmsRuntimeVersion.V4_1 );

        assertRuntimeVersion( DbmsRuntimeVersion.V4_1 );

        systemDb.executeTransactionally( "CALL dbms.upgrade()" );

        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
    }

    @Test
    void shouldUpgradeKernelVersionInTransactionOnRuntimeUpgrade() throws Exception
    {
        //Given
        setRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        setKernelVersion( KernelVersion.V4_2 );
        restartDatabase();
        long startTransaction = cluster.awaitLeader().resolveDependency( DEFAULT_DATABASE_NAME, TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        assertKernelVersion( KernelVersion.V4_2 );
        doWriteTransaction();

        //When
        systemDb.executeTransactionally( "CALL dbms.upgrade()" );

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertKernelVersion( KernelVersion.V4_2 );

        //When
        doWriteTransaction();

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertKernelVersion( KernelVersion.LATEST );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
    }

    @Test
    void shouldUpgradeKernelVersionInTransactionOnRuntimeUpgradeStressTest() throws Throwable
    {
        //Given
        setRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        setKernelVersion( KernelVersion.V4_2 );
        restartDatabase();
        long startTransaction = cluster.awaitLeader().resolveDependency( DEFAULT_DATABASE_NAME, TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        assertKernelVersion( KernelVersion.V4_2 );
        doWriteTransaction(); // Just to have at least one tx from our measurement point in the old version

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( () -> isKernelVersion( KernelVersion.LATEST ) );
        race.addContestant( () ->
        {
            systemDb.executeTransactionally( "CALL dbms.upgrade()" );
        }, 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), throwing( () -> {
            doWriteTransaction();
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertKernelVersion( KernelVersion.LATEST );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
    }

    @Test
    void shouldUpgradeKernelVersionInTransactionOnDenseNodeOnRuntimeUpgradeStressTest() throws Throwable
    {
        //Given
        setRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        setKernelVersion( KernelVersion.V4_2 );
        restartDatabase();
        long startTransaction = cluster.awaitLeader().resolveDependency( DEFAULT_DATABASE_NAME, TransactionIdStore.class ).getLastCommittedTransactionId();

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.V4_2 );
        assertKernelVersion( KernelVersion.V4_2 );
        long nodeId = createDenseNode();

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( new BooleanSupplier()
        {
            private final AtomicLong timeOfUpgrade = new AtomicLong();

            @Override
            public boolean getAsBoolean()
            {
                if ( isKernelVersion( KernelVersion.V4_3_D3 ) )
                {
                    // Notice the time of upgrade...
                    timeOfUpgrade.compareAndSet( 0, currentTimeMillis() );
                }
                // ... and continue a little while after it happened so that we get transactions both before and after
                return timeOfUpgrade.get() != 0 && (currentTimeMillis() - timeOfUpgrade.get()) > 1_000;
            }
        } );
        race.addContestant( throwing( () ->
        {
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 1_000 ) );
            systemDb.executeTransactionally( "CALL dbms.upgrade()" );
        } ), 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), throwing( () -> {
            addRelationshipToNode( nodeId );
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertKernelVersion( KernelVersion.LATEST );
        assertDegrees( nodeId );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
    }

    private void assertDegrees( long nodeId )
    {
        // Why assert degrees specifically? This particular upgrade: V4_2 -> V4_3_D3 changes how dense node degrees are stored
        // so it's a really good indicator that everything there works
        Map<String,Map<Direction,MutableLong>> referenceActualDegrees = null;
        for ( ClusterMember member : cluster.coreMembers() )
        {
            try ( InternalTransaction tx = member.database( DEFAULT_DATABASE_NAME ).beginTransaction(
                    KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
            {
                Node node = tx.getNodeById( nodeId );
                Map<String,Map<Direction,MutableLong>> actualDegrees = new HashMap<>();
                node.getRelationships().forEach(
                        r -> actualDegrees.computeIfAbsent( r.getType().name(), t -> new HashMap<>() ).computeIfAbsent( directionOf( node, r ),
                                d -> new MutableLong() ).increment() );
                MutableLong actualTotalDegree = new MutableLong();
                actualDegrees.forEach( ( typeName, directions ) ->
                {
                    long actualTotalDirectionDegree = 0;
                    for ( Map.Entry<Direction,MutableLong> actualDirectionDegree : directions.entrySet() )
                    {
                        assertThat( node.getDegree( RelationshipType.withName( typeName ), actualDirectionDegree.getKey() ) ).isEqualTo(
                                actualDirectionDegree.getValue().longValue() );
                        actualTotalDirectionDegree += actualDirectionDegree.getValue().longValue();
                    }
                    assertThat( node.getDegree( RelationshipType.withName( typeName ) ) ).isEqualTo( actualTotalDirectionDegree );
                    actualTotalDegree.add( actualTotalDirectionDegree );
                } );
                assertThat( node.getDegree() ).isEqualTo( actualTotalDegree.longValue() );

                if ( referenceActualDegrees == null )
                {
                    referenceActualDegrees = actualDegrees;
                }
                else
                {
                    assertThat( actualDegrees ).isEqualTo( referenceActualDegrees );
                }
            }
        }
    }

    private Direction directionOf( Node node, Relationship relationship )
    {
        return relationship.getStartNode().equals( node ) ? relationship.getEndNode().equals( node ) ? Direction.BOTH : Direction.OUTGOING : Direction.INCOMING;
    }

    private long createDenseNode() throws Exception
    {
        MutableLong nodeId = new MutableLong();
        cluster.coreTx( ( db, transaction ) ->
        {
            Node node = transaction.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                node.createRelationshipTo( transaction.createNode(), RelationshipType.withName( "TYPE_" + (i % 3) ) );
            }
            nodeId.setValue( node.getId() );
            transaction.commit();
        } );
        return nodeId.longValue();
    }

    private void addRelationshipToNode( long nodeId ) throws Exception
    {
        cluster.coreTx( ( db, transaction ) ->
        {
            transaction.getNodeById( nodeId ).createRelationshipTo( transaction.createNode(),
                    RelationshipType.withName( "TYPE_" + ThreadLocalRandom.current().nextInt( 3 ) ) );
            transaction.commit();
        } );
    }

    private void doWriteTransaction() throws Exception
    {
        cluster.coreTx( ( db, transaction ) ->
        {
            transaction.createNode();
            transaction.commit();
        } );
    }

    private void assertUpgradeTransactionInOrder( KernelVersion from, KernelVersion to, long fromTxId ) throws Exception
    {
        stopDatabase( DEFAULT_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStopped( DEFAULT_DATABASE_NAME, cluster );

        for ( ClusterMember member : cluster.allMembers() )
        {
            ArrayList<KernelVersion> transactionVersions = new ArrayList<>();
            ArrayList<CommittedTransactionRepresentation> transactions = new ArrayList<>();

            LogFile logFile = LogFilesBuilder.logFilesBasedOnlyBuilder( member.databaseLayout().getTransactionLogsDirectory(), fs ).build().getLogFile();
            try ( PhysicalLogVersionedStoreChannel reader = logFile.openForVersion( 0 );
                    ReadAheadLogChannel readAheadLogChannel = new ReadAheadLogChannel( reader, new ReaderLogVersionBridge( logFile ), INSTANCE );
                    PhysicalTransactionCursor cursor = new PhysicalTransactionCursor( readAheadLogChannel,
                        new VersionAwareLogEntryReader( StorageEngineFactory.selectStorageEngine().commandReaderFactory() ) ) )
            {
                while ( cursor.next() )
                {
                    CommittedTransactionRepresentation representation = cursor.get();
                    if ( representation.getCommitEntry().getTxId() > fromTxId )
                    {
                        transactions.add( representation );
                        transactionVersions.add( representation.getStartEntry().getVersion() );
                    }
                }
            }
            assertThat( transactionVersions ).hasSizeGreaterThanOrEqualTo( 2 ); //at least upgrade transaction and the triggering transaction
            assertThat( transactionVersions ).isSortedAccordingTo( Comparator.comparingInt( KernelVersion::version ) ); //Sorted means everything is in order
            assertThat( transactionVersions.get( 0 ) ).isEqualTo( from ); //First should be "from" version
            assertThat( transactionVersions.get( transactionVersions.size() - 1 ) ).isEqualTo( to ); //And last the "to" version

            CommittedTransactionRepresentation upgradeTransaction = transactions.get( transactionVersions.indexOf( to ) );
            PhysicalTransactionRepresentation physicalRep = (PhysicalTransactionRepresentation) upgradeTransaction.getTransactionRepresentation();
            physicalRep.accept( element ->
            {
                assertThat( element ).isInstanceOf( Command.MetaDataCommand.class );
                return true;
            } );
        }
    }

    private void setRuntimeVersion( DbmsRuntimeVersion runtimeVersion )
    {
        try ( var tx = systemDb.beginTx() )
        {
            tx.findNodes( VERSION_LABEL )
              .stream()
              .forEach( dbmsRuntimeNode -> dbmsRuntimeNode.setProperty( DBMS_RUNTIME_COMPONENT, runtimeVersion.getVersion() ) );

            tx.commit();
        }

        doOnAllDbmsRuntimeRepositories( dbmsRuntimeRepository -> dbmsRuntimeRepository.setVersion( runtimeVersion ) );
    }

    private void assertRuntimeVersion( DbmsRuntimeVersion expectedRuntimeVersion )
    {
        doOnAllDbmsRuntimeRepositories(
                dbmsRuntimeRepository -> assertEventually( dbmsRuntimeRepository::getVersion, equalityCondition( expectedRuntimeVersion ), 20, SECONDS )
        );
    }

    private void setKernelVersion( KernelVersion kernelVersion )
    {
        doOnAllMetaDataStore( metaDataStore -> metaDataStore.setKernelVersion( kernelVersion, PageCursorTracer.NULL ) );
    }

    private void assertKernelVersion( KernelVersion expectedKernelVersion )
    {
        doOnAllMetaDataStore( metaDataStore -> assertEventually( metaDataStore::kernelVersion, equalityCondition( expectedKernelVersion ), 20, SECONDS ) );
    }

    private void doOnAllDbmsRuntimeRepositories( Consumer<DbmsRuntimeRepository> repositoryConsumer )
    {
        cluster.allMembers().stream()
               .map( clusterMember -> clusterMember.resolveDependency( SYSTEM_DATABASE_NAME, DbmsRuntimeRepository.class ) )
               .forEach( repositoryConsumer );
    }

    private boolean isKernelVersion( KernelVersion kernelVersion )
    {
        MutableBoolean allMatch = new MutableBoolean( true );
        doOnAllMetaDataStore( mds -> allMatch.setValue( allMatch.booleanValue() && mds.kernelVersion().equals( kernelVersion ) ) );
        return allMatch.booleanValue();
    }

    private void doOnAllMetaDataStore( Consumer<MetaDataStore> metaDataConsumer )
    {
        cluster.allMembers().stream()
                .map( clusterMember -> clusterMember.resolveDependency( DEFAULT_DATABASE_NAME, MetaDataStore.class ) )
                .forEach( metaDataConsumer );
    }

    private void restartDatabase() throws Exception
    {
        stopDatabase( DEFAULT_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStopped( DEFAULT_DATABASE_NAME, cluster );
        startDatabase( DEFAULT_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );
    }
}
