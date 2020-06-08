/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestWithRecordFormats;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.cluster_topology_refresh;
import static com.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static com.neo4j.causalclustering.discovery.IpFamily.IPV6;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_listen_address;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( TestInstance.Lifecycle.PER_METHOD )
class OnlineBackupCommandCcIT
{
    @Inject
    private SuppressOutput suppressOutput;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private ClusterFactory clusterFactory;

    private File backupsDir;

    @BeforeEach
    void setUp()
    {
        backupsDir = testDirectory.directory( "backups" );
    }

    @TestWithRecordFormats
    void backupCanBePerformedOverCcWithCustomPort( String recordFormat ) throws Exception
    {
        Cluster cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( cluster.awaitLeader().defaultDatabase() );

        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );
        assertEquals( leaderDbRepresentation( cluster ), getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );

        createSomeData( cluster );
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );
        assertEquals( leaderDbRepresentation( cluster ), getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );
    }

    @TestWithRecordFormats
    void dataIsInAUsableStateAfterBackup( String recordFormat ) throws Exception
    {
        // given database exists
        Cluster cluster = startCluster( recordFormat );

        // and the database has indexes
        createIndexes( cluster );

        // and the database is being populated
        AtomicBoolean populateDatabaseFlag = new AtomicBoolean( true );
        Thread thread = new Thread( () -> repeatedlyPopulateDatabase( cluster, populateDatabaseFlag ) );
        thread.start(); // populate db with number properties etc.
        try
        {
            // then backup is successful
            String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();
            assertEquals( 0, runBackupToolAndGetExitCode( address, DEFAULT_DATABASE_NAME ) );
        }
        finally
        {
            populateDatabaseFlag.set( false );
            thread.join();
        }
    }

    @TestWithRecordFormats
    void backupCanBeOptionallySwitchedOnWithTheBackupConfig( String recordFormat ) throws Exception
    {
        // given a cluster with backup switched on
        ClusterConfig clusterConfig = defaultClusterConfig( recordFormat )
                .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, TRUE )
                .withInstanceCoreParam( online_backup_listen_address, i -> "localhost:" + PortAuthority.allocatePort() );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        // when a full backup is performed
        assertEquals( 0, runBackupToolAndGetExitCode( leaderBackupAddress( cluster ), DEFAULT_DATABASE_NAME ) );
        assertEquals( leaderDbRepresentation( cluster ), getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );

        // and an incremental backup is performed
        createSomeData( cluster );
        assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( leaderBackupAddress( cluster ), DEFAULT_DATABASE_NAME ) );

        // then the data matches
        assertEquals( leaderDbRepresentation( cluster ), getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );
    }

    @TestWithRecordFormats
    void secondaryTransactionProtocolIsSwitchedOffCorrespondingBackupSetting( String recordFormat ) throws Exception
    {
        // given a cluster with backup switched off
        ClusterConfig clusterConfig = defaultClusterConfig( recordFormat )
                .withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, FALSE )
                .withInstanceCoreParam( online_backup_listen_address, i -> "localhost:" + PortAuthority.allocatePort() );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        // then a full backup is impossible from the backup port
        assertEquals( 1, runBackupToolAndGetExitCode( leaderBackupAddress( cluster ), DEFAULT_DATABASE_NAME ) );
    }

    @TestWithRecordFormats
    void backupDoesntDisplayExceptionWhenSuccessful( String recordFormat ) throws Exception
    {
        // given
        Cluster cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( cluster.awaitLeader().defaultDatabase() );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );

        // then
        assertThat( suppressOutput.getErrorVoice().toString() ).doesNotContain( "Exception" );
        assertThat( suppressOutput.getOutputVoice().toString() ).doesNotContain( "Exception" );
    }

    @TestWithRecordFormats
    void reportsProgress( String recordFormat ) throws Exception
    {
        // given
        Cluster cluster = startCluster( recordFormat );
        createIndexes( cluster );
        String customAddress = CausalClusteringTestHelpers.backupAddress( cluster.awaitLeader().defaultDatabase() );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );

        // then
        String output = suppressOutput.getOutputVoice().toString();

        assertThat( output ).contains( "Start receiving store files" );
        assertThat( output ).contains( "Finish receiving store files" );

        String nodeStorePath = backupsDir.toPath().resolve( DEFAULT_DATABASE_NAME ).resolve( DatabaseFile.NODE_STORE.getName() ).toString();
        assertThat( output ).contains( "Start receiving store file " + nodeStorePath );
        assertThat( output ).contains( "Finish receiving store file " + nodeStorePath );

        assertThat( output ).contains( "Start receiving transactions from" );
        assertThat( output ).contains( "Finish receiving transactions at" );
    }

    @TestWithRecordFormats
    void fullBackupIsRecoveredAndConsistent( String recordFormat ) throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();

        File backupLocation = new File( backupsDir, DEFAULT_DATABASE_NAME );
        DatabaseLayout backupLayout = DatabaseLayout.ofFlat( backupLocation );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( address, DEFAULT_DATABASE_NAME ) );

        // then
        assertFalse( isRecoveryRequired( backupLayout ), "Store should not require recovery" );
        assertTrue( isConsistent( backupLayout ), "Consistency check failed" );
    }

    @TestWithRecordFormats
    void incrementalBackupIsRecoveredAndConsistent( String recordFormat ) throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();

        File backupLocation = new File( backupsDir, DEFAULT_DATABASE_NAME );
        DatabaseLayout backupLayout = DatabaseLayout.ofFlat( backupLocation );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( address, DEFAULT_DATABASE_NAME ) );

        // and
        createSomeData( cluster );
        assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( address, DEFAULT_DATABASE_NAME ) );

        // then
        assertFalse( isRecoveryRequired( backupLayout ), "Store should not require recovery" );
        assertTrue( isConsistent( backupLayout ), "Consistency check failed" );
    }

    @TestWithRecordFormats
    void incrementalBackupRotatesLogFiles( String recordFormat ) throws Exception
    {
        // given database exists with data
        Cluster cluster = startCluster( recordFormat );
        createSomeData( cluster );

        // and backup client is told to rotate conveniently
        File configOverrideFile = testDirectory.file( "neo4j-backup.conf" );
        writeConfigWithLogRotationThreshold( configOverrideFile, "1m" );

        // and we have a full backup
        File backupDir = testDirectory.directory( "backups", "backupName-" + recordFormat );
        String address = CausalClusteringTestHelpers.backupAddress( cluster.awaitLeader().defaultDatabase() );
        assertEquals( 0, runBackupToolAndGetExitCode(
                "--from", address,
                "--report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--additional-config=" + configOverrideFile,
                "--database=" + DEFAULT_DATABASE_NAME ) );

        LogFiles backupLogFiles = logFilesFromBackup( backupDir, DEFAULT_DATABASE_NAME );
        assertEquals( 0, backupLogFiles.getHighestLogVersion() );
        assertEquals( 0, backupLogFiles.getLowestLogVersion() );

        // and the database contains a few more transactions that cause backup to perform a rotation
        insert20MbOfData( cluster );

        // when we perform an incremental backup
        assertEquals( 0, runBackupToolAndGetExitCode(
                "--from", address,
                "--report-dir=" + backupDir,
                "--backup-dir=" + backupDir,
                "--additional-config=" + configOverrideFile,
                "--database=" + DEFAULT_DATABASE_NAME,
                "--fallback-to-full=false" ) );

        // then there has been a rotation
        assertThat( backupLogFiles.getHighestLogVersion() ).isGreaterThan( 0L );
        // and the original log has not been removed since the transactions are applied at start
        assertEquals( 0, backupLogFiles.getLowestLogVersion() );
    }

    @TestWithRecordFormats
    void backupRenamesWork( String recordFormat ) throws Exception
    {
        // given a prexisting backup from a different store
        Cluster cluster1 = startCluster( recordFormat );
        String firstBackupAddress = CausalClusteringTestHelpers.transactionAddress( cluster1.awaitLeader().defaultDatabase() );

        assertEquals( 0, runBackupToolAndGetExitCode( firstBackupAddress, DEFAULT_DATABASE_NAME ) );
        DbRepresentation firstDatabaseRepresentation = leaderDbRepresentation( cluster1 );

        // and a different database
        Cluster cluster2 = startSecondCluster( recordFormat );
        DbRepresentation secondDatabaseRepresentation = DbRepresentation.of( cluster2.awaitLeader().defaultDatabase() );
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
        String secondBackupAddress = CausalClusteringTestHelpers.transactionAddress( cluster2.awaitLeader().defaultDatabase() );

        // when backup is performed
        assertEquals( 0, runBackupToolAndGetExitCode( secondBackupAddress, DEFAULT_DATABASE_NAME ) );
        cluster2.shutdown();

        // then the new backup has the correct name
        assertEquals( secondDatabaseRepresentation, getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );

        // and the old backup is in a renamed location
        assertEquals( firstDatabaseRepresentation, getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME + ".err.0" ) );

        // and the data isn't equal (sanity check)
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
    }

    @TestWithRecordFormats
    void ipv6Enabled( String recordFormat ) throws Exception
    {
        // given
        Cluster cluster = startIpv6Cluster( recordFormat );
        int port = cluster.awaitLeader().config().get( CausalClusteringSettings.transaction_listen_address ).getPort();
        String customAddress = String.format( "[%s]:%d", IPV6.localhostAddress(), port );

        // when full backup
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );

        // and
        createSomeData( cluster );

        // and incremental backup
        assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( customAddress, DEFAULT_DATABASE_NAME ) );

        // then
        assertEquals( leaderDbRepresentation( cluster ), getBackupDbRepresentation( backupsDir, DEFAULT_DATABASE_NAME ) );
    }

    private static void repeatedlyPopulateDatabase( Cluster cluster, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            createSomeData( cluster );
        }
    }

    private static DbRepresentation leaderDbRepresentation( Cluster cluster ) throws Exception
    {
        return DbRepresentation.of( cluster.awaitLeader().defaultDatabase() );
    }

    private Cluster startCluster( String recordFormat ) throws Exception
    {
        ClusterConfig clusterConfig = defaultClusterConfig( recordFormat );
        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private static ClusterConfig defaultClusterConfig( String recordFormat )
    {
        return ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( cluster_topology_refresh, "5s" )
                .withSharedCoreParam( record_format, recordFormat )
                .withSharedReadReplicaParam( record_format, recordFormat );
    }

    private Cluster startIpv6Cluster( String recordFormat ) throws ExecutionException, InterruptedException
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParam( record_format, recordFormat )
                .withSharedReadReplicaParam( record_format, recordFormat )
                .withIpFamily( IPV6 )
                .useWildcard( false );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster startSecondCluster( String recordFormat ) throws ExecutionException, InterruptedException
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( record_format, recordFormat )
                .withSharedReadReplicaParam( record_format, recordFormat )
                .withIpFamily( IPV4 )
                .useWildcard( false );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private static void insert20MbOfData( Cluster cluster ) throws Exception
    {
        int numberOfTransactions = 500;
        int sizeOfTransaction = (int) ((ByteUnit.mebiBytes( 20 ) / numberOfTransactions) + 1);
        byte[] data = new byte[sizeOfTransaction];
        Arrays.fill( data, (byte) 42 );
        for ( int txId = 0; txId < numberOfTransactions; txId++ )
        {
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                Node node = transaction.createNode();
                node.setProperty( "data", data );
                transaction.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
                transaction.commit();
            } );
        }
    }

    private static boolean isConsistent( DatabaseLayout backupLayout ) throws ConsistencyCheckIncompleteException
    {
        Config config = Config.defaults();
        ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.textual( System.out );
        LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        boolean checkRelationshipTypeScanStore = config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store );
        ConsistencyFlags flags = new ConsistencyFlags( true, true, true, true, checkRelationshipTypeScanStore, true );
        ConsistencyCheckService service = new ConsistencyCheckService();
        ConsistencyCheckService.Result result = service.runFullConsistencyCheck( backupLayout, config, progressMonitorFactory, logProvider, true, flags );
        return result.isSuccessful();
    }

    private static DbRepresentation getBackupDbRepresentation( File backupDir, String databaseName )
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, false );
        return DbRepresentation.of( DatabaseLayout.ofFlat( new File( backupDir, databaseName ) ), config );
    }

    private int runBackupToolAndGetExitCode( String address, String databaseName )
    {
        return runBackupToolAndGetExitCode(
                "--from", address,
                "--report-dir=" + backupsDir,
                "--backup-dir=" + backupsDir,
                "--database=" + databaseName );
    }

    private int runBackupToolWithoutFallbackToFullAndGetExitCode( String address, String databaseName )
    {
        return runBackupToolAndGetExitCode(
                "--from", address,
                "--report-dir=" + backupsDir,
                "--backup-dir=" + backupsDir,
                "--database=" + databaseName,
                "--fallback-to-full=false" );
    }

    private int runBackupToolAndGetExitCode( String... args )
    {
        File neo4jHome = testDirectory.absolutePath();
        return BackupTestUtil.runBackupToolFromSameJvm( neo4jHome, args );
    }

    private static LogFiles logFilesFromBackup( File backupDir, String databaseName ) throws IOException
    {
        DatabaseLayout backupLayout = DatabaseLayout.ofFlat( new File( backupDir, databaseName ) );
        return BackupTransactionLogFilesHelper.readLogFiles( backupLayout );
    }

    private boolean isRecoveryRequired( DatabaseLayout layout ) throws Exception
    {
        return Recovery.isRecoveryRequired( testDirectory.getFileSystem(), layout, Config.defaults(), INSTANCE );
    }

    private static String leaderBackupAddress( Cluster cluster ) throws TimeoutException
    {
        SocketAddress address = cluster.awaitLeader().config().get( online_backup_listen_address );
        assertNotNull( address );
        return address.toString();
    }

    private static void writeConfigWithLogRotationThreshold( File conf, String value ) throws IOException
    {
        Files.writeString( conf.toPath(), logical_log_rotation_threshold.name() + "=" + value );
    }

    private static void createIndexes( Cluster cluster ) throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            tx.schema().indexFor( label( "Person" ) ).on( "id" ).create();
            tx.schema().indexFor( label( "Person" ) ).on( "first_name" ).on( "last_name" ).create();
            tx.commit();
        } );
    }

    private static void createSomeData( Cluster cluster )
    {
        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = tx.createNode( label( "Person" ) );
                node.setProperty( "id", ThreadLocalRandom.current().nextLong() );
                node.setProperty( "first_name", UUID.randomUUID().toString() );
                node.setProperty( "last_name", UUID.randomUUID().toString() );
                tx.createNode( label( "Person" ) ).createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
                tx.commit();
            } );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
