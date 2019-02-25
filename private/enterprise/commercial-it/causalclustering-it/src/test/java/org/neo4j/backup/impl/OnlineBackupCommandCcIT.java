/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DefaultCluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import com.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.causalclustering.ClusterRule;
import com.neo4j.util.TestHelpers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import org.neo4j.backup.clusteringsupport.ClusterHelper;
import org.neo4j.common.Service;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_listen_address;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

@RunWith( Parameterized.class )
public class OnlineBackupCommandCcIT
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final RuleChain ruleChain =
            RuleChain.outerRule( suppressOutput ).around( fileSystemRule ).around( testDirectory ).around( pageCacheRule ).around( clusterRule );

    private static final String DATABASE_NAME = "defaultport";
    private File backupDatabaseDir;
    private File backupStoreDir;
    private final StorageEngineFactory storageEngineFactory = selectStorageEngine( Service.loadAll( StorageEngineFactory.class ) );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void initialiseBackupDirectory()
    {
        backupStoreDir = testDirectory.directory( "backupStore" );
        backupDatabaseDir = new File( backupStoreDir, DATABASE_NAME );
        assertTrue( backupDatabaseDir.mkdirs() );
    }

    @Test
    public void backupCanBePerformedOverCcWithCustomPort() throws Exception
    {
        Cluster<?> cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );

        createSomeData( cluster );
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );
    }

    @Test
    public void dataIsInAUsableStateAfterBackup() throws Exception
    {
        // given database exists
        Cluster<?> cluster = startCluster( recordFormat );

        // and the database has indexes
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );

        // and the database is being populated
        AtomicBoolean populateDatabaseFlag = new AtomicBoolean( true );
        Thread thread = new Thread( () -> repeatedlyPopulateDatabase( cluster, populateDatabaseFlag ) );
        thread.start(); // populate db with number properties etc.
        try
        {
            // then backup is successful
            String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();
            assertEquals( 0, runBackupToolAndGetExitCode( address, DATABASE_NAME ) );
        }
        finally
        {
            populateDatabaseFlag.set( false );
            thread.join();
        }
    }

    @Test
    public void backupCanBeOptionallySwitchedOnWithTheBackupConfig() throws Exception
    {
        // given a cluster with backup switched on
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "true" )
                .withInstanceCoreParam( online_backup_listen_address, i -> "localhost:" + PortAuthority.allocatePort() );
        Cluster<?> cluster = startCluster( recordFormat );

        // when a full backup is performed
        assertEquals( 0, runBackupToolAndGetExitCode( leaderBackupAddress( cluster ), DATABASE_NAME ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );

        // and an incremental backup is performed
        createSomeData( cluster );
        assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( leaderBackupAddress( cluster ), DATABASE_NAME ) );

        // then the data matches
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( DATABASE_NAME, backupStoreDir ) );
    }

    @Test
    public void secondaryTransactionProtocolIsSwitchedOffCorrespondingBackupSetting() throws Exception
    {
        // given a cluster with backup switched off
        clusterRule = clusterRule.withSharedCoreParam( OnlineBackupSettings.online_backup_enabled, "false" )
                .withInstanceCoreParam( online_backup_listen_address, i -> "localhost:" + PortAuthority.allocatePort() );
        Cluster<?> cluster = startCluster( recordFormat );

        // then a full backup is impossible from the backup port
        assertEquals( 1, runBackupToolAndGetExitCode( leaderBackupAddress( cluster ), DATABASE_NAME ) );
    }

    @Test
    public void backupDoesntDisplayExceptionWhenSuccessful() throws Exception
    {
        // given
        Cluster<?> cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, DATABASE_NAME ) );

        // then
        assertFalse( suppressOutput.getErrorVoice().toString().toLowerCase().contains( "exception" ) );
        assertFalse( suppressOutput.getOutputVoice().toString().toLowerCase().contains( "exception" ) );
    }

    @Test
    public void reportsProgress() throws Exception
    {
        // given
        Cluster<?> cluster = startCluster( recordFormat );
        ClusterHelper.createIndexes( cluster.getMemberWithAnyRole( Role.LEADER ).database() );
        String customAddress = CausalClusteringTestHelpers.backupAddress( clusterLeader( cluster ).database() );

        // when
        final String backupName = "reportsProgress_" + recordFormat;
        assertEquals( 0, runBackupToolAndGetExitCode( customAddress, backupName ) );

        // then
        String output = suppressOutput.getOutputVoice().toString();
        String location = Paths.get( backupStoreDir.toString(), backupName ).toString();

        assertTrue( output.contains( "Start receiving store files" ) );
        assertTrue( output.contains( "Finish receiving store files" ) );
        String tested = Paths.get( location, "neostore.nodestore.db.labels" ).toString();
        assertTrue( tested, output.contains( format( "Start receiving store file %s", tested ) ) );
        assertTrue( tested, output.contains( format( "Finish receiving store file %s", tested ) ) );
        assertTrue( output.contains( "Start receiving transactions from " ) );
        assertTrue( output.contains( "Finish receiving transactions at " ) );
        assertTrue( output.contains( "Start receiving index snapshots" ) );
        assertTrue( output.contains( "Finished receiving index snapshots" ) );
    }

    @Test
    public void fullBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists with data
        Cluster<?> cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupStoreDir, name );
        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( address, name ) );

        // then
        assertFalse( "Store should not require recovery", isRecoveryRequired( fileSystemRule, backupLayout, Config.defaults(), storageEngineFactory ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLayout, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void incrementalBackupIsRecoveredAndConsistent() throws Exception
    {
        // given database exists with data
        Cluster<?> cluster = startCluster( recordFormat );
        createSomeData( cluster );
        String address = cluster.awaitLeader().config().get( online_backup_listen_address ).toString();

        String name = UUID.randomUUID().toString();
        File backupLocation = new File( backupStoreDir, name );
        DatabaseLayout backupLayout = DatabaseLayout.of( backupLocation );

        // when
        assertEquals( 0, runBackupToolAndGetExitCode( address, name ) );

        // and
        createSomeData( cluster );
        assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( address, name ) );

        // then
        assertFalse( "Store should not require recovery", isRecoveryRequired( fileSystemRule, backupLayout, Config.defaults(), storageEngineFactory ) );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        assertTrue( "Consistency check failed", new ConsistencyCheckService()
                .runFullConsistencyCheck( backupLayout, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, consistencyFlags )
                .isSuccessful() );
    }

    @Test
    public void incrementalBackupRotatesLogFiles() throws Exception
    {
        // given database exists with data
        Cluster<?> cluster = startCluster( recordFormat );
        createSomeData( cluster );

        // and backup client is told to rotate conveniently
        File configOverrideFile = testDirectory.file( "neo4j-backup.conf" );
        writeConfigWithLogRotationThreshold( configOverrideFile, "1m" );

        // and we have a full backup
        final String backupName = "backupName" + recordFormat;
        String address = CausalClusteringTestHelpers.backupAddress( clusterLeader( cluster ).database() );
        assertEquals( 0, runBackupToolAndGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName ) );

        LogFiles backupLogFiles = logFilesFromBackup( backupName );
        assertEquals( 0, backupLogFiles.getHighestLogVersion() );
        assertEquals( 0, backupLogFiles.getLowestLogVersion() );

        // and the database contains a few more transactions that cause backup to perform a rotation
        insert20MbOfData( cluster );

        // when we perform an incremental backup
        assertEquals( 0, runBackupToolAndGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--additional-config=" + configOverrideFile,
                "--name=" + backupName,
                "--fallback-to-full=false" ) );

        // then there has been a rotation
        assertThat( backupLogFiles.getHighestLogVersion(), greaterThan( 0L ) );
        // and the original log has not been removed since the transactions are applied at start
        assertEquals( 0, backupLogFiles.getLowestLogVersion() );
    }

    @Test
    public void backupRenamesWork() throws Exception
    {
        // given a prexisting backup from a different store
        String backupName = "preexistingBackup_" + recordFormat;
        Cluster<?> cluster = startCluster( recordFormat );
        String firstBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );

        assertEquals( 0, runBackupToolAndGetExitCode( firstBackupAddress, backupName ) );
        DbRepresentation firstDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster ).database() );

        // and a different database
        Cluster<?> cluster2 = startCluster2( recordFormat );
        DbRepresentation secondDatabaseRepresentation = DbRepresentation.of( clusterLeader( cluster2 ).database() );
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
        String secondBackupAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster2 ).database() );

        // when backup is performed
        assertEquals( 0, runBackupToolAndGetExitCode( secondBackupAddress, backupName ) );
        cluster2.shutdown();

        // then the new backup has the correct name
        assertEquals( secondDatabaseRepresentation, getBackupDbRepresentation( backupName, backupStoreDir ) );

        // and the old backup is in a renamed location
        assertEquals( firstDatabaseRepresentation, getBackupDbRepresentation( backupName + ".err.0", backupStoreDir ) );

        // and the data isn't equal (sanity check)
        assertNotEquals( firstDatabaseRepresentation, secondDatabaseRepresentation );
    }

    @Test
    public void ipv6Enabled() throws Exception
    {
        // given
        Cluster<?> cluster = startIpv6Cluster();
        try
        {
            assertNotNull( DbRepresentation.of( clusterDatabase( cluster ) ) );
            int port = clusterLeader( cluster ).config().get( CausalClusteringSettings.transaction_listen_address ).getPort();
            String customAddress = String.format( "[%s]:%d", IpFamily.IPV6.localhostAddress(), port );
            String backupName = "backup_" + recordFormat;

            // when full backup
            assertEquals( 0, runBackupToolAndGetExitCode( customAddress, backupName ) );

            // and
            createSomeData( cluster );

            // and incremental backup
            assertEquals( 0, runBackupToolWithoutFallbackToFullAndGetExitCode( customAddress, backupName ) );

            // then
            assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( backupName, backupStoreDir ) );
        }
        finally
        {
            cluster.shutdown();
        }
    }

    private static void repeatedlyPopulateDatabase( Cluster<?> cluster, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            createSomeData( cluster );
        }
    }

    public static CoreGraphDatabase clusterDatabase( Cluster<?> cluster )
    {
        return clusterLeader( cluster ).database();
    }

    private Cluster<?> startCluster( String recordFormat ) throws Exception
    {
        ClusterRule clusterRule = this.clusterRule.withSharedCoreParam( GraphDatabaseSettings.record_format, recordFormat )
                .withSharedReadReplicaParam( GraphDatabaseSettings.record_format, recordFormat );
        Cluster<?> cluster = clusterRule.startCluster();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster<?> startIpv6Cluster() throws ExecutionException, InterruptedException
    {
        DiscoveryServiceFactory discoveryServiceFactory = new SharedDiscoveryServiceFactory();
        File parentDir = testDirectory.directory( "ipv6_cluster" );
        Map<String,String> coreParams = new HashMap<>();
        coreParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();

        Map<String,String> readReplicaParams = new HashMap<>();
        readReplicaParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();

        Cluster<?> cluster = new DefaultCluster( parentDir, 3, 3, discoveryServiceFactory, coreParams, instanceCoreParams, readReplicaParams,
                instanceReadReplicaParams, recordFormat, IpFamily.IPV6, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private Cluster<?> startCluster2( String recordFormat ) throws ExecutionException, InterruptedException
    {
        Map<String,String> sharedParams = new HashMap<>(  );
        sharedParams.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        Cluster<?> cluster =
                new DefaultCluster( testDirectory.directory( "cluster-b_" + recordFormat ), 3, 0, new SharedDiscoveryServiceFactory(),
                        sharedParams, emptyMap(), sharedParams, emptyMap(),
                recordFormat, IpFamily.IPV4, false );
        cluster.start();
        createSomeData( cluster );
        return cluster;
    }

    private static void insert20MbOfData( Cluster<?> cluster ) throws Exception
    {
        int numberOfTransactions = 500;
        int sizeOfTransaction = (int) ((ByteUnit.mebiBytes( 20 ) / numberOfTransactions) + 1);
        byte[] data = new byte[sizeOfTransaction];
        Arrays.fill( data, (byte) 42 );
        for ( int txId = 0; txId < numberOfTransactions; txId++ )
        {
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                Node node = coreGraphDatabase.createNode();
                node.setProperty( "data", data );
                coreGraphDatabase.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
                transaction.success();
            } );
        }
    }

    public static void createSomeData( Cluster<?> cluster )
    {
        try
        {
            cluster.coreTx( ClusterHelper::createSomeData );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static CoreClusterMember clusterLeader( Cluster<?> cluster )
    {
        return cluster.getMemberWithRole( Role.LEADER );
    }

    public static DbRepresentation getBackupDbRepresentation( String name, File storeDir )
    {
        Config config = Config.defaults();
        config.augment( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( DatabaseLayout.of( storeDir, name ).databaseDirectory(), config );
    }

    private int runBackupToolAndGetExitCode( String address, String backupName )
    {
        return runBackupToolAndGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--name=" + backupName );
    }

    private int runBackupToolWithoutFallbackToFullAndGetExitCode( String address, String backupName )
    {
        return runBackupToolAndGetExitCode(
                "--from", address,
                "--cc-report-dir=" + backupStoreDir,
                "--backup-dir=" + backupStoreDir,
                "--name=" + backupName,
                "--fallback-to-full=false" );
    }

    private int runBackupToolAndGetExitCode( String... args )
    {
        File neo4jHome = testDirectory.absolutePath();
        return TestHelpers.runBackupToolFromSameJvm( neo4jHome, args );
    }

    private LogFiles logFilesFromBackup( String backupName ) throws IOException
    {
        DatabaseLayout backupLayout = DatabaseLayout.of( new File( backupStoreDir, backupName ) );
        return BackupTransactionLogFilesHelper.readLogFiles( backupLayout );
    }

    private static String leaderBackupAddress( Cluster<?> cluster ) throws TimeoutException
    {
        ListenSocketAddress address = cluster.awaitLeader().config().get( online_backup_listen_address );
        assertNotNull( address );
        return address.toString();
    }

    private static void writeConfigWithLogRotationThreshold( File conf, String value ) throws IOException
    {
        Config config = Config.builder()
                .withSetting( GraphDatabaseSettings.logical_log_rotation_threshold, value )
                .build();

        try ( BufferedWriter writer = Files.newBufferedWriter( conf.toPath() ) )
        {
            Properties properties = new Properties();
            properties.putAll( config.getRaw() );
            properties.store( writer, "" );
        }
    }
}
