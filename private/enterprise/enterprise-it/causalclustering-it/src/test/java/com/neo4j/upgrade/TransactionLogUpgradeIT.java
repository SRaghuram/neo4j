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
import org.agrona.collections.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
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

        //When
        Race race = new Race().withRandomStartDelays().withEndCondition( () -> isKernelVersion( KernelVersion.LATEST ) );
        race.addContestant( () ->
        {
            systemDb.executeTransactionally( "CALL dbms.upgrade()" );
        }, 1 );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), Race.throwing( () -> {
            doWriteTransaction();
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0, 2 ) );
        } ) );
        race.go( 1, TimeUnit.MINUTES );

        //Then
        assertRuntimeVersion( DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION );
        assertKernelVersion( KernelVersion.LATEST );
        assertUpgradeTransactionInOrder( KernelVersion.V4_2, KernelVersion.LATEST, startTransaction );
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
        for ( ClusterMember member : cluster.allMembers() )
        {
            LogicalTransactionStore lts = member.resolveDependency( DEFAULT_DATABASE_NAME, LogicalTransactionStore.class );
            ArrayList<KernelVersion> transactionVersions = new ArrayList<>();
            ArrayList<CommittedTransactionRepresentation> transactions = new ArrayList<>();
            try ( TransactionCursor transactionCursor = lts.getTransactions( fromTxId + 1 ) )
            {
                while ( transactionCursor.next() )
                {
                    CommittedTransactionRepresentation representation = transactionCursor.get();
                    transactions.add( representation );
                    transactionVersions.add( representation.getStartEntry().getVersion() );
                }
            }
            assertThat( transactionVersions ).hasSizeGreaterThanOrEqualTo( 2 ); //at least upgrade transaction and the triggering transaction
            assertThat( transactionVersions ).isSortedAccordingTo( Comparator.comparingInt( KernelVersion::version ) ); //Sorted means everything is in order
            assertThat( transactionVersions.get( 0 ) ).isEqualTo( from ); //First should be "from" version
            assertThat( transactionVersions.get( transactionVersions.size() - 1 ) ).isEqualTo( to ); //And last the "to" version

            CommittedTransactionRepresentation upgradeTransaction = transactions.get( transactionVersions.indexOf( to ) - 1 );
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
        doOnAllMetaDataStore( mds -> allMatch.set( allMatch.get() && mds.kernelVersion().equals( kernelVersion ) ) );
        return allMatch.get();
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
