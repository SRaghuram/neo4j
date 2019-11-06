/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.ssl.SslPolicyScope.BACKUP;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

@ExtendWith( SuppressOutputExtension.class )
@TestDirectoryExtension
@ClusterExtension
abstract class BaseEncryptedBackupIT
{
    private static final int BACKUP_SSL_START = 6; // certs for backup start after 6
    private static final String BACKUP_POLICY_NAME = "backup";
    private static final String CLUSTER_POLICY_NAME = "cluster";
    private static final String PUBLIC_KEY_NAME = "public.crt";

    @Inject
    private TestDirectory testDir;
    @Inject
    private ClusterFactory clusterFactory;

    private final boolean encryptedTxPort;
    private final boolean encryptedBackupPort;

    private FileSystemAbstraction fs;
    private Cluster cluster;
    private File backupHome;

    BaseEncryptedBackupIT( boolean encryptedTxPort, boolean encryptedBackupPort )
    {
        this.encryptedTxPort = encryptedTxPort;
        this.encryptedBackupPort = encryptedBackupPort;
    }

    @BeforeAll
    void beforeAll() throws Exception
    {
        fs = testDir.getFileSystem();
        cluster = initialiseCluster( encryptedTxPort, encryptedBackupPort );
    }

    @BeforeEach
    void beforeEach()
    {
        backupHome = testDir.directory( "backupNeo4jHome-" + randomUUID().toString() );
    }

    @Test
    void unencryptedBackupAgainstTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
        if ( encryptedTxPort )
        {
            shouldNotBeSuccessful( backupClient );
        }
        else
        {
            shouldBeSuccessful( cluster, backupClient );
        }
    }

    @Test
    void unencryptedBackupAgainstReplicaTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
        if ( encryptedTxPort )
        {
            shouldNotBeSuccessful( backupClient );
        }
        else
        {
            shouldBeSuccessful( cluster, backupClient );
        }
    }

    @Test
    void unencryptedBackupAgainstBackupAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
        if ( encryptedBackupPort )
        {
            shouldNotBeSuccessful( backupClient );
        }
        else
        {
            shouldBeSuccessful( cluster, backupClient );
        }
    }

    @Test
    void unencryptedBackupAgainstReplicaBackupAddress() throws Exception
    {
        var backupClient = backupClientWithoutEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
        if ( encryptedBackupPort )
        {
            shouldNotBeSuccessful( backupClient );
        }
        else
        {
            shouldBeSuccessful( cluster, backupClient );
        }
    }

    @Test
    void transactionEncryptedBackupAgainstTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithClusterEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
        if ( encryptedTxPort )
        {
            shouldBeSuccessful( cluster, backupClient );
        }
        else
        {
            shouldNotBeSuccessful( backupClient );
        }
    }

    @Test
    void transactionEncryptedBackupAgainstReplicaTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithClusterEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
        if ( encryptedTxPort )
        {
            shouldBeSuccessful( cluster, backupClient );
        }
        else
        {
            shouldNotBeSuccessful( backupClient );
        }
    }

    @Test
    void transactionEncryptedBackupAgainstBackupAddress() throws Exception
    {
        var backupClient = backupClientWithClusterEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
        shouldNotBeSuccessful( backupClient ); // keys shouldn't match
    }

    @Test
    void transactionEncryptedBackupAgainstReplicaBackupAddress() throws Exception
    {
        var backupClient = backupClientWithClusterEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
        shouldNotBeSuccessful( backupClient ); // keys shouldn't match
    }

    @Test
    void backupEncryptedBackupAgainstTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithBackupEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
        shouldNotBeSuccessful( backupClient ); // keys shouldn't match
    }

    @Test
    void backupEncryptedBackupAgainstReplicaTransactionAddress() throws Exception
    {
        var backupClient = backupClientWithBackupEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
        shouldNotBeSuccessful( backupClient ); // keys shouldn't match
    }

    @Test
    void backupEncryptedBackupAgainstBackupAddress() throws Exception
    {
        var backupClient = backupClientWithBackupEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
        if ( encryptedBackupPort )
        {
            shouldBeSuccessful( cluster, backupClient );
        }
        else
        {
            shouldNotBeSuccessful( backupClient );
        }
    }

    @Test
    void backupEncryptedBackupAgainstReplicaBackupAddress() throws Exception
    {
        var backupClient = backupClientWithBackupEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
        if ( encryptedBackupPort )
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
                .withNumberOfReadReplicas( 3 )
                .withSharedCoreParams( memberSettings )
                .withSharedReadReplicaParams( memberSettings );

        return clusterFactory.createCluster( clusterConfig );
    }

    private IntSupplier backupClientWithoutEncryption( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.empty(), false );
    }

    private IntSupplier backupClientWithClusterEncryption( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.of( 0 ), false );
    }

    private IntSupplier backupClientWithBackupEncryption( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.of( BACKUP_SSL_START ), false );
    }

    private IntSupplier backupClientWithBackupEncryptionToReplica( Cluster cluster, Setting<?> addressSetting ) throws Exception
    {
        return backupClient( cluster, addressSetting, OptionalInt.of( BACKUP_SSL_START ), true );
    }

    private IntSupplier backupClientWithClusterEncryptionToReplica( Cluster cluster, Setting<?> addressSetting ) throws Exception
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
            exchangeBackupClientKeyWithCluster( cluster, backupHome, selectedSslKey > 6 ? BACKUP_POLICY_NAME : CLUSTER_POLICY_NAME );
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

    private void exchangeBackupClientKeyWithCluster( Cluster cluster, File backupHome, String targetPolicyName ) throws IOException
    {
        // Copy backup cert to cluster trusted and copy cluster keys to backup
        for ( var clusterMember : cluster.allMembers() )
        {
            copySslToPolicyTrustedDirectory( backupHome, clusterMember.homeDir(), BACKUP_POLICY_NAME, targetPolicyName, "backup-client.crt" );
            copySslToPolicyTrustedDirectory( clusterMember.homeDir(), backupHome, targetPolicyName, BACKUP_POLICY_NAME,
                    "from-cluster-" + clusterUniqueServerId( clusterMember ) + ".crt" );
        }
    }

    private void installCryptographicObjectsToBackupHome( File neo4jHome, int keyId ) throws IOException
    {
        createConfigFile( neo4jHome );
        var certificatesLocation = neo4jHome.toPath().resolve( "certificates" ).resolve( BACKUP_POLICY_NAME ).toFile();
        fs.mkdirs( certificatesLocation );
        installSsl( certificatesLocation, keyId );
        copySslToPolicyTrustedDirectory( neo4jHome, neo4jHome, BACKUP_POLICY_NAME, "backup-key-copy.crt" );
    }

    private void createConfigFile( File neo4jHome ) throws IOException
    {
        var config = neo4jHome.toPath().resolve( "conf" + File.separator + Config.DEFAULT_CONFIG_FILE_NAME ).toFile();
        var backupPolicyLocation = neo4jHome.toPath().resolve( "certificates" ).resolve( "backup" ).toFile();
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
        var sslResourceBuilder = SslResourceBuilder.selfSignedKeyId( keyId );
        trustInGroup( sslResourceBuilder, keyId ).install( baseDir );
    }

    private static SslResourceBuilder trustInGroup( SslResourceBuilder sslResourceBuilder, int keyId )
    {
        var groupBaseId = keyId - (keyId % 6);
        for ( var mutualSignId = 0; mutualSignId < 6; mutualSignId++ )
        {
            sslResourceBuilder = sslResourceBuilder.trustKeyId( groupBaseId + mutualSignId );
        }
        return sslResourceBuilder;
    }

    /**
     * It is necessary to run from the same jvm due to being dependant on ssl which is enterprise edition only
     */
    private static int runBackupSameJvm( File neo4jHome, String host )
    {
        return BackupTestUtil.runBackupToolFromSameJvm( neo4jHome,
                "--from", host,
                "--backup-dir", neo4jHome.toString(),
                "--database", DEFAULT_DATABASE_NAME );
    }

    // ---------------------- New functionality

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
        for ( var sourceClusterMember : cluster.allMembers() )
        {
            for ( var targetClusterMember : cluster.allMembers() )
            {
                long sourceKeyId = baseKey + clusterUniqueServerId( sourceClusterMember );
                long targetKeyId = baseKey + clusterUniqueServerId( targetClusterMember );
                var targetKeyMarkedAsAllowed = format( "public-%d-%d-cluster-trusted.crt", sourceKeyId, targetKeyId );
                copySslToPolicyTrustedDirectory( sourceClusterMember.homeDir(), targetClusterMember.homeDir(), policyName, targetKeyMarkedAsAllowed );
            }
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
        var homeDir = member.homeDir();
        var policyDir = createPolicyDirectories( homeDir, policyName );
        createSslInParent( policyDir, keyId );
    }

    private File createPolicyDirectories( File homeDir, String policyName ) throws IOException
    {
        var policyDir = new File( homeDir, "certificates/" + policyName );
        fs.mkdirs( new File( policyDir, "trusted" ) );
        fs.mkdirs( new File( policyDir, "revoked" ) );
        return policyDir;
    }

    private static void createSslInParent( File policyDir, int keyId ) throws IOException
    {
        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( policyDir );
    }

    private void copySslToPolicyTrustedDirectory( File sourceHome, File targetHome, String policyName, String targetFileName ) throws IOException
    {
        copySslToPolicyTrustedDirectory( sourceHome, targetHome, policyName, policyName, targetFileName );
    }

    private void copySslToPolicyTrustedDirectory( File sourceHome, File targetHome, String sourcePolicyName, String targetPolicyName, String targetFileName )
            throws IOException
    {
        var sourcePublicKey = Path.of( sourceHome.getPath(), "certificates", sourcePolicyName, PUBLIC_KEY_NAME );
        var targetPublicKey = Path.of( targetHome.getPath(), "certificates", targetPolicyName, "trusted", targetFileName );
        fs.mkdirs( targetPublicKey.toFile().getParentFile() );
        try
        {
            if ( fs.fileExists( sourcePublicKey.toFile() ) )
            {
                fs.copyFile( sourcePublicKey.toFile(), targetPublicKey.toFile(), StandardCopyOption.REPLACE_EXISTING );
            }
        }
        catch ( RuntimeException e )
        {
            throw new RuntimeException( format( "\nFileA: %s\nFileB: %s\n", sourcePublicKey, targetPublicKey ), e );
        }
    }
}
