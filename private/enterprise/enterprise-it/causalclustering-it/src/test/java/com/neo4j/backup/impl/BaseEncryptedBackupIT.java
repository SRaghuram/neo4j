/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.IntSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.ssl.SslPolicyScope.BACKUP;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

@ExtendWith( SuppressOutputExtension.class )
@TestDirectoryExtension
@ClusterExtension
@ResourceLock( Resources.SYSTEM_OUT )
abstract class BaseEncryptedBackupIT
{
    private static final String BACKUP_POLICY_NAME = "backup";
    private static final String CLUSTER_POLICY_NAME = "cluster";

    @Inject
    private TestDirectory testDir;
    @Inject
    private ClusterFactory clusterFactory;

    private final boolean serverHasEncryptedTxPort;
    private final boolean serverHasEncryptedBackupPort;

    private FileSystemAbstraction fs;
    private Cluster cluster;
    private Path backupHome;

    BaseEncryptedBackupIT( boolean serverHasEncryptedTxPort, boolean serverHasEncryptedBackupPort )
    {
        this.serverHasEncryptedTxPort = serverHasEncryptedTxPort;
        this.serverHasEncryptedBackupPort = serverHasEncryptedBackupPort;
    }

    @BeforeAll
    void beforeAll() throws Exception
    {
        fs = testDir.getFileSystem();
        cluster = initialiseCluster( serverHasEncryptedTxPort, serverHasEncryptedBackupPort );
    }

    @BeforeEach
    void beforeEach()
    {
        backupHome = testDir.directoryPath( "backupNeo4jHome-" + randomUUID().toString() );
    }

    @Test
    void unencryptedBackupAgainstTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
        testNotEncryptedBackupClient( backupClient, serverHasEncryptedTxPort );
    }

    @Test
    void unencryptedBackupAgainstReplicaTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
        testNotEncryptedBackupClient( backupClient, serverHasEncryptedTxPort );
    }

    @Test
    void unencryptedBackupAgainstBackupAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
        testNotEncryptedBackupClient( backupClient, serverHasEncryptedBackupPort );
    }

    @Test
    void unencryptedBackupAgainstReplicaBackupAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
        testNotEncryptedBackupClient( backupClient, serverHasEncryptedBackupPort );
    }

    @Test
    void encryptedBackupAgainstTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
        testEncryptedBackupClient( backupClient, serverHasEncryptedTxPort );
    }

    @Test
    void encryptedBackupAgainstReplicaTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
        testEncryptedBackupClient( backupClient, serverHasEncryptedTxPort );
    }

    @Test
    void encryptedBackupAgainstBackupAddress() throws Exception
    {
        var backupClient = backupClientWithEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
        testEncryptedBackupClient( backupClient, serverHasEncryptedBackupPort );
    }

    @Test
    void encryptedBackupAgainstReplicaBackupAddress() throws Exception
    {
        var backupClient = backupClientWithEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
        testEncryptedBackupClient( backupClient, serverHasEncryptedBackupPort );
    }

    private void testNotEncryptedBackupClient( IntSupplier backupClient, boolean encryptedServerPort ) throws Exception
    {
        if ( encryptedServerPort )
        {
            shouldNotBeSuccessful( backupClient );
        }
        else
        {
            shouldBeSuccessful( cluster, backupClient );
        }
    }

    private void testEncryptedBackupClient( IntSupplier backupClient, boolean encryptedServerPort ) throws Exception
    {
        if ( encryptedServerPort )
        {
            shouldBeSuccessful( cluster, backupClient );
        }
        else
        {
            shouldNotBeSuccessful( backupClient );
        }
    }

    private Cluster initialiseCluster( boolean encryptedTxPort, boolean encryptedBackupPort ) throws Exception
    {
        var memberSettings = clusterMemberSettingsWithEncryption( encryptedTxPort, encryptedBackupPort );
        var cluster = createCluster( memberSettings );
        setupClusterWithEncryption( cluster, encryptedTxPort, encryptedBackupPort );
        cluster.start();
        DataCreator.createDataInOneTransaction( cluster, 100 );
        return cluster;
    }

    private void shouldBeSuccessful( Cluster cluster, IntSupplier backupClient ) throws Exception
    {
        // when a full backup is successful
        var exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // and the cluster is populated with more data
        DataCreator.createDataInOneTransaction( cluster, 100 );
        dataMatchesEventually( cluster.awaitLeader(), cluster.allMembers() );

        // then an incremental backup is successful on that cluster
        exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // and data matches
        assertEquals( DbRepresentation.of( cluster.awaitLeader().defaultDatabase() ),
                DbRepresentation.of( Neo4jLayout.ofFlat( backupHome ).databaseLayout( DEFAULT_DATABASE_NAME ) ) );
    }

    private static void shouldNotBeSuccessful( IntSupplier backupClient )
    {
        // when
        var exitCode = backupClient.getAsInt();

        // then backup fails because certificate is rejected
        assertEquals( 1, exitCode );
    }

    private Cluster createCluster( Map<String,String> memberSettings )
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParams( memberSettings )
                .withSharedReadReplicaParams( memberSettings );

        return clusterFactory.createCluster( clusterConfig );
    }

    private IntSupplier backupClientWithoutEncryption( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.empty(), false );
    }

    private IntSupplier backupClientWithEncryption( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.of( 0 ), false );
    }

    private IntSupplier backupClientWithEncryptionToReplica( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.of( 0 ), true );
    }

    private IntSupplier backupClientWithoutEncryptionToReplica( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.empty(), true );
    }

    /**
     * This creates a backup client that is easy to invoke and correctly configured.
     * The backup address used is read from the leader at runtime. If the leaders config has backup enabled -> the backup address is used; Otherwise
     * the transaction port is used. The configuration of the encryption remains the same.
     *
     * @param baseSslKeyId If this is empty then there is no encryption, otherwise it may be configured for either policy (as an offset)
     * @return a function that can be used to invoke the client. Return integer is backup exit code
     * @throws IOException if there was an error with non-backup code
     */
    private IntSupplier backupClient( Cluster cluster, Setting<?> addressSetting, OptionalInt baseSslKeyId, boolean replicaOnly ) throws Exception
    {
        // and backup client is configured
        ClusterMember selectedNode;
        if ( replicaOnly )
        {
            cluster.awaitLeader();
            selectedNode = cluster.findAnyReadReplica();
        }
        else
        {
            selectedNode = cluster.awaitLeader();
        }
        var serverId = clusterUniqueServerId( selectedNode );
        var selectedNodeAddress = selectedNode.settingValue( addressSetting ).toString();
        if ( baseSslKeyId.isPresent() )
        {
            var selectedSslKey = serverId + baseSslKeyId.getAsInt();
            installCryptographicObjectsToBackupHome( backupHome, selectedSslKey );
        }
        // when a full backup is successful
        return () ->
        {
            try
            {
                dataMatchesEventually( cluster.awaitLeader(), cluster.allMembers() );
                return runBackupSameJvm( backupHome, selectedNodeAddress );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        };
    }

    private void installCryptographicObjectsToBackupHome( Path neo4jHome, int keyId ) throws IOException
    {
        createConfigFile( neo4jHome );
        var certificatesLocation = neo4jHome.resolve( "certificates" ).resolve( BACKUP_POLICY_NAME ).toFile();
        fs.mkdirs( certificatesLocation );
        installSsl( certificatesLocation, keyId );
    }

    private void createConfigFile( Path neo4jHome ) throws IOException
    {
        var config = neo4jHome.resolve( "conf" + File.separator + Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        var backupPolicyLocation = neo4jHome.resolve( "certificates" ).resolve( "backup" ).toFile();
        fs.mkdirs( backupPolicyLocation );
        var properties = new Properties();
        var backupSslConfigGroup = SslPolicyConfig.forScope( BACKUP );
        properties.setProperty( backupSslConfigGroup.enabled.name(), TRUE );
        properties.setProperty( backupSslConfigGroup.base_directory.name(), backupPolicyLocation.getAbsolutePath() );
        fs.mkdirs( config.getParentFile() );

        try ( var fileWriter = new FileWriter( config ) )
        {
            properties.store( fileWriter, StringUtils.EMPTY );
        }
    }

    private void installSsl( File baseDir, int keyId ) throws IOException
    {
        fs.mkdirs( new File( baseDir, "trusted" ) );
        fs.mkdirs( new File( baseDir, "revoked" ) );
        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( baseDir );
    }

    /**
     * It is necessary to run from the same jvm due to being dependant on ssl which is enterprise edition only
     */
    private static int runBackupSameJvm( Path neo4jHome, String host )
    {
        return BackupTestUtil.runBackupToolFromSameJvm( neo4jHome,
                "--from", host,
                "--backup-dir", neo4jHome.toString(),
                "--database", DEFAULT_DATABASE_NAME );
    }

    private static Map<String,String> clusterMemberSettingsWithEncryption( boolean encryptedTx, boolean encryptedBackup )
    {
        var settings = new HashMap<String,String>();
        settings.put( OnlineBackupSettings.online_backup_enabled.name(), TRUE );
        if ( encryptedTx )
        {
            configureClusterConfigEncryptedCluster( settings );
        }
        if ( encryptedBackup )
        {
            configureClusterConfigEncryptedBackup( settings );
        }
        return settings;
    }

    private void setupClusterWithEncryption( Cluster cluster, boolean encryptedTx, boolean encryptedBackup ) throws IOException
    {
        if ( encryptedTx )
        {
            setupEntireClusterTrusted( cluster, CLUSTER_POLICY_NAME, 0 );
        }
        if ( encryptedBackup )
        {
            setupEntireClusterTrusted( cluster, BACKUP_POLICY_NAME, 6 );
        }
    }

    private static void configureClusterConfigEncryptedCluster( Map<String,String> settings )
    {
        settings.put( SslPolicyConfig.forScope( CLUSTER ).enabled.name(), TRUE );
        settings.put( SslPolicyConfig.forScope( CLUSTER ).base_directory.name(), Path.of( "certificates/" + CLUSTER_POLICY_NAME ).toString() );
    }

    private static void configureClusterConfigEncryptedBackup( Map<String,String> settings )
    {
        settings.put( SslPolicyConfig.forScope( BACKUP ).enabled.name(), TRUE );
        settings.put( SslPolicyConfig.forScope( BACKUP ).base_directory.name(), Path.of( "certificates/" + BACKUP_POLICY_NAME ).toString() );
    }

    private void setupEntireClusterTrusted( Cluster cluster, String policyName, int baseKey ) throws IOException
    {
        for ( var clusterMember : cluster.allMembers() )
        {
            prepareCoreToHaveKeys( clusterMember, baseKey + clusterUniqueServerId( clusterMember ), policyName );
        }
    }

    private static int clusterUniqueServerId( ClusterMember clusterMember )
    {
        if ( clusterMember instanceof CoreClusterMember )
        {
            return clusterMember.serverId();
        }
        var numberOfCores = 3;
        return clusterMember.serverId() + numberOfCores;
    }

    private void prepareCoreToHaveKeys( ClusterMember member, int keyId, String policyName ) throws IOException
    {
        var homeDir = member.homePath();
        var policyDir = createPolicyDirectories( homeDir, policyName );
        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( policyDir.toFile() );
    }

    private Path createPolicyDirectories( Path homeDir, String policyName ) throws IOException
    {
        var policyDir = homeDir.resolve( "certificates/" + policyName );
        fs.mkdirs( policyDir.resolve( "revoked" ).toFile() );
        fs.mkdirs( policyDir.resolve( "trusted" ).toFile() );
        return policyDir;
    }
}
