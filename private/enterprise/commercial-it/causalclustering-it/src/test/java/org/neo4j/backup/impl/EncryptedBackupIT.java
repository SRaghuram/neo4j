/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.IntSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.BaseSslPolicyConfig;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryClassExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, SuppressOutputExtension.class} )
class EncryptedBackupIT
{
    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    @DisplayName( "Cluster with encryptedTx=true encryptedBackup=true" )
    class TrueTrue extends Context
    {
        TrueTrue()
        {
            super( true, true );
        }
    }

    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    @DisplayName( "Cluster with encryptedTx=true encryptedBackup=false" )
    class TrueFalse extends Context
    {
        TrueFalse()
        {
            super( true, false );
        }
    }

    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    @DisplayName( "Cluster with encryptedTx=false encryptedBackup=false" )
    class FalseFalse extends Context
    {
        FalseFalse()
        {
            super( false, false );
        }
    }

    @Nested
    @TestInstance( TestInstance.Lifecycle.PER_METHOD )
    @DisplayName( "Cluster with encryptedTx=false encryptedBackup=true" )
    class FalseTrue extends Context
    {
        FalseTrue()
        {
            super( false, true );
        }
    }

    static class Context
    {
        @RegisterExtension
        public static TestDirectoryClassExtension testDirectoryClassExtension = new TestDirectoryClassExtension();

        private static DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

        private static final int BACKUP_SSL_START = 6; // certs for backup start after 6
        private static final String backupPolicyName = "backup";
        private static final String clusterPolicyName = "cluster";
        private static final String publicKeyName = "public.crt";

        private static File backupHome;

        boolean encryptedTxPort;
        boolean encryptedBackupPort;
        static Cluster cluster;

        Context( boolean encryptedTxPort, boolean encryptedBackupPort )
        {
            this.encryptedTxPort = encryptedTxPort;
            this.encryptedBackupPort = encryptedBackupPort;
        }

        @BeforeEach
        void init() throws Exception
        {
            backupHome = testDirectoryClassExtension.getTestDirectory().directory( "backupNeo4jHome-" + UUID.randomUUID().toString() );

            // NOTE this causes tests to no be able to be run in parallel
            if ( cluster == null )
            {
                cluster = initialiseCluster( encryptedTxPort, encryptedBackupPort );
            }
        }

        @AfterAll
        static void shutdown()
        {
            if ( cluster != null )
            {
                cluster.shutdown();
                cluster = null;
            }
        }

        @Test
        void unencryptedBackupAgainstTransactionAddress() throws Exception
        {
            IntSupplier backupClient = backupClientWithoutEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
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
            IntSupplier backupClient = backupClientWithoutEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
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
            IntSupplier backupClient = backupClientWithoutEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
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
            IntSupplier backupClient = backupClientWithoutEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
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
            IntSupplier backupClient = backupClientWithClusterEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
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
            IntSupplier backupClient = backupClientWithClusterEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
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
            IntSupplier backupClient = backupClientWithClusterEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
            shouldNotBeSuccessful( backupClient ); // keys shouldn't match
        }

        @Test
        void transactionEncryptedBackupAgainstReplicaBackupAddress() throws Exception
        {
            IntSupplier backupClient = backupClientWithClusterEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
            shouldNotBeSuccessful( backupClient ); // keys shouldn't match
        }

        @Test
        void backupEncryptedBackupAgainstTransactionAddress() throws Exception
        {
            IntSupplier backupClient = backupClientWithBackupEncryption( cluster, CausalClusteringSettings.transaction_listen_address );
            shouldNotBeSuccessful( backupClient ); // keys shouldn't match
        }

        @Test
        void backupEncryptedBackupAgainstReplicaTransactionAddress() throws Exception
        {
            IntSupplier backupClient = backupClientWithBackupEncryptionToReplica( cluster, CausalClusteringSettings.transaction_listen_address );
            shouldNotBeSuccessful( backupClient ); // keys shouldn't match
        }

        @Test
        void backupEncryptedBackupAgainstBackupAddress() throws Exception
        {
            IntSupplier backupClient = backupClientWithBackupEncryption( cluster, OnlineBackupSettings.online_backup_listen_address );
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
            IntSupplier backupClient = backupClientWithBackupEncryptionToReplica( cluster, OnlineBackupSettings.online_backup_listen_address );
            if ( encryptedBackupPort )
            {
                shouldBeSuccessful( cluster, backupClient );
            }
            else
            {
                shouldNotBeSuccessful( backupClient );
            }
        }

        private static Cluster initialiseCluster( boolean encryptedTxPort, boolean encryptedBackupPort ) throws Exception
        {
            Cluster cluster = aCluster( testDirectoryClassExtension.getTestDirectory() );
            try
            {
                setupClusterWithEncryption( cluster, encryptedTxPort, encryptedBackupPort );
                cluster.start();
            }
            catch ( IOException | InterruptedException | ExecutionException e )
            {
                throw new RuntimeException( e );
            }
            DataCreator.createDataInOneTransaction( cluster, 100 );
            return cluster;
        }

        private static void shouldBeSuccessful( Cluster cluster, IntSupplier backupClient ) throws Exception
        {
            // when a full backup is successful
            int exitCode = backupClient.getAsInt();
            assertEquals( 0, exitCode );

            // and the cluster is populated with more data
            DataCreator.createDataInOneTransaction( cluster, 100 );
            dataMatchesEventually( cluster.getMemberWithRole( Role.LEADER ), allMembers( cluster ) );

            // then an incremental backup is successful on that cluster
            exitCode = backupClient.getAsInt();
            assertEquals( 0, exitCode );

            // and data matches
            assertEquals( DbRepresentation.of( cluster.awaitLeader().database() ), DbRepresentation.of( new File( backupHome, DEFAULT_DATABASE_NAME ) ) );
        }

        private static void shouldNotBeSuccessful( IntSupplier backupClient )
        {
            // when
            int exitCode = backupClient.getAsInt();

            // then backup fails because certificate is rejected
            assertEquals( 1, exitCode );
        }

        private static Cluster aCluster( TestDirectory testDir )
        {
            int noOfCoreMembers = 3;
            int noOfReadReplicas = 3;

            // use secure discovery service factory to make cluster members communicate with SSL enabled
            DiscoveryServiceFactory discoveryServiceFactory = new SslHazelcastDiscoveryServiceFactory();
            return new Cluster( testDir.directory( UUID.randomUUID().toString() ), noOfCoreMembers, noOfReadReplicas,
                    discoveryServiceFactory, emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
        }

        private static IntSupplier backupClientWithoutEncryption( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.empty(), false );
        }

        private static IntSupplier backupClientWithClusterEncryption( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.of( 0 ), false );
        }

        private static IntSupplier backupClientWithBackupEncryption( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.of( BACKUP_SSL_START ), false );
        }

        private static IntSupplier backupClientWithBackupEncryptionToReplica( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.of( BACKUP_SSL_START ), true );
        }

        private static IntSupplier backupClientWithClusterEncryptionToReplica( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.of( 0 ), true );
        }

        private static IntSupplier backupClientWithoutEncryptionToReplica( Cluster cluster, Setting addressSetting ) throws Exception
        {
            return backupClient( cluster, addressSetting, Optional.empty(), true );
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
        private static IntSupplier backupClient( Cluster cluster, Setting addressSetting, Optional<Integer> baseSslKeyId, boolean replicaOnly )
                throws Exception
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
            int serverId = clusterUniqueServerId( selectedNode );
            String selectedNodeAddress = selectedNode.settingValue( addressSetting.name() );
            if ( baseSslKeyId.isPresent() )
            {
                int selectedSslKey = serverId + baseSslKeyId.get();
                installCryptographicObjectsToBackupHome( backupHome, selectedSslKey );
                exchangeBackupClientKeyWithCluster( cluster, backupHome, selectedSslKey > 6 ? backupPolicyName : clusterPolicyName );
            }
            // when a full backup is successful
            return () ->
            {
                try
                {
                    dataMatchesEventually( cluster.getMemberWithRole( Role.LEADER ), allMembers( cluster ) );
                    return runBackupSameJvm( backupHome, selectedNodeAddress );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            };
        }

        private static void exchangeBackupClientKeyWithCluster( Cluster cluster, File backupHome, String targetPolicyName ) throws IOException
        {
            // Copy backup cert to cluster trusted and copy cluster keys to backup
            for ( ClusterMember clusterMember : allMembers( cluster ) )
            {
                copySslToPolicyTrustedDirectory( backupHome, clusterMember.homeDir(), backupPolicyName, targetPolicyName, "backup-client.crt" );
                copySslToPolicyTrustedDirectory( clusterMember.homeDir(), backupHome, targetPolicyName, backupPolicyName,
                        "from-cluster-" + clusterUniqueServerId( clusterMember ) + ".crt" );
            }
        }

        private static void installCryptographicObjectsToBackupHome( File neo4J_home, int keyId ) throws IOException
        {
            createConfigFile( neo4J_home );
            File certificatesLocation = neo4J_home.toPath().resolve( "certificates" ).resolve( backupPolicyName ).toFile();
            certificatesLocation.mkdirs();
            installSsl( fs, certificatesLocation, keyId );
            copySslToPolicyTrustedDirectory( neo4J_home, neo4J_home, backupPolicyName, "backup-key-copy.crt" );
        }

        private static void createConfigFile( File neo4J_home ) throws IOException
        {
            File config = neo4J_home.toPath().resolve( "conf" + File.separator + "neo4j.conf" ).toFile();
            File backupPolicyLocation = neo4J_home.toPath().resolve( "certificates" ).resolve( "backup" ).toFile();
            backupPolicyLocation.mkdirs();
            Properties properties = new Properties();
            BaseSslPolicyConfig backupSslConfigGroup = new PemSslPolicyConfig( backupPolicyName );
            properties.setProperty( OnlineBackupSettings.ssl_policy.name(), backupPolicyName );
            properties.setProperty( backupSslConfigGroup.base_directory.name(), backupPolicyLocation.getAbsolutePath() );
            properties.setProperty( backupSslConfigGroup.format.name(), BaseSslPolicyConfig.Format.PEM.name() );
            config.getParentFile().mkdirs();

            try ( FileWriter fileWriter = new FileWriter( config ) )
            {
                properties.store( fileWriter, StringUtils.EMPTY );
            }
            Properties debugConfig = debugConfigFile( config );
            System.out.println( "DEBUG: saved config file was " + debugConfig );
        }

        private static Properties debugConfigFile( File config ) throws IOException
        {
            Properties properties = new Properties();
            properties.load( new FileReader( config ) );
            return properties;
        }

        private static void installSsl( FileSystemAbstraction fs, File baseDir, int keyId ) throws IOException
        {
            fs.mkdirs( new File( baseDir, "trusted" ) );
            fs.mkdirs( new File( baseDir, "revoked" ) );
            SslResourceBuilder sslResourceBuilder = SslResourceBuilder.selfSignedKeyId( keyId );
            trustInGroup( sslResourceBuilder, keyId ).install( baseDir );
        }

        private static SslResourceBuilder trustInGroup( SslResourceBuilder sslResourceBuilder, int keyId )
        {
            int groupBaseId = keyId - (keyId % 6);
            for ( int mutualSignId = 0; mutualSignId < 6; mutualSignId++ )
            {
                sslResourceBuilder = sslResourceBuilder.trustKeyId( groupBaseId + mutualSignId );
            }
            return sslResourceBuilder;
        }

        /**
         * It is necessary to run from the same jvm due to being dependant on ssl which is commercial only
         */
        private static int runBackupSameJvm( File neo4jHome, String host )
        {
            return BackupTestUtil.runBackupToolFromSameJvm( neo4jHome,
                    "--from", host,
                    "--backup-dir", neo4jHome.toString(),
                    "--database", DEFAULT_DATABASE_NAME );
        }

        // ---------------------- New functionality

        private static void setupClusterWithEncryption( Cluster cluster, boolean encryptedTx, boolean encryptedBackup ) throws IOException
        {
            allMembers( cluster ).forEach( member -> member.config().augment( OnlineBackupSettings.online_backup_enabled, "true" ) );
            if ( encryptedTx )
            {
                configureClusterConfigEncryptedCluster( cluster );
                setupEntireClusterTrusted( cluster, clusterPolicyName, 0 );
            }
            if ( encryptedBackup )
            {
                configureClusterConfigEncryptedBackup( cluster );
                setupEntireClusterTrusted( cluster, backupPolicyName, 6 );
            }
        }

        private static void configureClusterConfigEncryptedCluster( Cluster cluster )
        {
            BaseSslPolicyConfig clusterPolicyConfig = new PemSslPolicyConfig( clusterPolicyName );
            Config additionalConf = Config.builder()
                    .withSetting( CausalClusteringSettings.ssl_policy, clusterPolicyName )
                    .withSetting( clusterPolicyConfig.format, BaseSslPolicyConfig.Format.PEM.name() )
                    .withSetting( clusterPolicyConfig.base_directory, "certificates/" + clusterPolicyName )
                    .build();
            for ( ClusterMember clusterMember : allMembers( cluster ) )
            {
                clusterMember.config().augment( additionalConf );
            }
        }

        private static void configureClusterConfigEncryptedBackup( Cluster cluster )
        {
            BaseSslPolicyConfig backupPolicyConfig = new PemSslPolicyConfig( backupPolicyName );
            Config additionalConf = Config.builder()
                    .withSetting( OnlineBackupSettings.ssl_policy, backupPolicyName )
                    .withSetting( backupPolicyConfig.format, BaseSslPolicyConfig.Format.PEM.name() )
                    .withSetting( backupPolicyConfig.base_directory, "certificates/" + backupPolicyName )
                    .build();
            for ( ClusterMember clusterMember : allMembers( cluster ) )
            {
                clusterMember.config().augment( additionalConf );
            }
        }

        private static Collection<ClusterMember> allMembers( Cluster cluster )
        {
            Collection<ClusterMember> members = new ArrayList<>();
            members.addAll( cluster.coreMembers() );
            members.addAll( cluster.readReplicas() );
            return members;
        }

        private static void setupEntireClusterTrusted( Cluster cluster, String policyName, int baseKey ) throws IOException
        {
            for ( ClusterMember clusterMember : allMembers( cluster ) )
            {
                prepareCoreToHaveKeys( clusterMember, baseKey + clusterUniqueServerId( clusterMember ), policyName );
            }
            for ( ClusterMember sourceClusterMember : allMembers( cluster ) )
            {
                for ( ClusterMember targetClusterMember : allMembers( cluster ) )
                {
                    long sourceKeyId = baseKey + clusterUniqueServerId( sourceClusterMember );
                    long targetKeyId = baseKey + clusterUniqueServerId( targetClusterMember );
                    String targetKeyMarkedAsAllowed = format( "public-%d-%d-cluster-trusted.crt", sourceKeyId, targetKeyId );
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
            int numberOfCores = 3;
            return clusterMember.serverId() + numberOfCores;
        }

        private static void prepareCoreToHaveKeys( ClusterMember member, int keyId, String policyName ) throws IOException
        {
            File homeDir = member.homeDir();
            File policyDir = createPolicyDirectories( fs, homeDir, policyName );
            createSslInParent( policyDir, keyId );
        }

        private static File createPolicyDirectories( FileSystemAbstraction fs, File homeDir, String policyName ) throws IOException
        {
            File policyDir = new File( homeDir, "certificates/" + policyName );
            fs.mkdirs( new File( policyDir, "trusted" ) );
            fs.mkdirs( new File( policyDir, "revoked" ) );
            return policyDir;
        }

        private static void createSslInParent( File policyDir, int keyId ) throws IOException
        {
            SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( policyDir );
        }

        private static void copySslToPolicyTrustedDirectory( File sourceHome, File targetHome, String policyName, String targetFileName ) throws IOException
        {
            copySslToPolicyTrustedDirectory( sourceHome, targetHome, policyName, policyName, targetFileName );
        }

        private static boolean copySslToPolicyTrustedDirectory( File sourceHome, File targetHome, String sourcePolicyName, String targetPolicyName,
                String targetFileName ) throws IOException
        {
            Path sourcePublicKey = Paths.get( sourceHome.getPath(), "certificates", sourcePolicyName, publicKeyName );
            Path targetPublicKey = Paths.get( targetHome.getPath(), "certificates", targetPolicyName, "trusted", targetFileName );
            System.out.printf( "Copying from %s to %s\n", sourcePublicKey, targetPublicKey );
            targetPublicKey.toFile().getParentFile().mkdirs();
            try
            {
                Files.copy( sourcePublicKey, targetPublicKey, StandardCopyOption.REPLACE_EXISTING );
                return true;
            }
            catch ( NoSuchFileException e )
            {
                new RuntimeException( format( "Certificate is missing: %s", sourcePublicKey ), e ).printStackTrace( System.out );
            }
            catch ( RuntimeException e )
            {
                new RuntimeException( format( "\nFileA: %s\nFileB: %s\n", sourcePublicKey, targetPublicKey ), e ).printStackTrace( System.out );
            }
            return false;
        }
    }
}
