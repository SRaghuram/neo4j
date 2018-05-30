/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.backup.impl.SelectedBackupProtocol;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.clusterDatabase;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.createSomeData;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.getBackupDbRepresentation;

public class EncryptedBackupIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster cluster;

    private static final int BACKUP_SSL_START = 6; // certs for backup start after 6
    private static final String backupPolicyName = "backup";
    private static final String clusterPolicyName = "cluster";
    private static final String publicKeyName = "public.crt";

    private File backupHome;
    private String backupName;

    @Before
    public void setup()
    {
        backupHome = testDir.directory( "backupNeo4jHome" );
        backupName = "encryptedBackup";
    }

    @After
    public void cleanup()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void backupsArePossibleFromEncryptedCluster() throws Exception
    {
        // given there exists an encrypted cluster
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();

        // and backup client is configured
        IntSupplier backupClient = backupClientWithClusterEncryption( CausalClusteringSettings.transaction_listen_address );

        // when a full backup is successful
        int exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // then data matches
        backupDataMatchesDatabase( cluster, backupHome, backupName );

        // when the cluster is populated with more data
        createSomeData( cluster );
        Cluster.dataMatchesEventually( cluster.getMemberWithRole( Role.LEADER ), cluster.coreMembers() );

        // then an incremental backup is successful on that cluster
        exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // and data matches
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void encryptedBackupsArePossibleFromBackupPort() throws Exception
    {
        // given there exists an encrypted cluster with exposed backup port
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();

        // and backup client is configured
        IntSupplier backupClient = backupClientWithBackupEncryption( OnlineBackupSettings.online_backup_server );

        // when a full backup is successful
        int exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // then data matches
        backupDataMatchesDatabase( cluster, backupHome, backupName );

        // when the cluster is populated with more data
        createSomeData( cluster );
        Cluster.dataMatchesEventually( cluster.getMemberWithRole( Role.LEADER ), cluster.coreMembers() );

        // then an incremental backup is successful on that cluster
        exitCode = backupClient.getAsInt();
        assertEquals( 0, exitCode );

        // and data matches
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void noPolicyAgainstBackupPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given backup port has ssl
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and backup client isn't encrypted
        IntSupplier backupClient = backupClientWithoutEncryption( OnlineBackupSettings.online_backup_server );

        // when backup against backup port
        int exitCode = backupClient.getAsInt();

        // then backup isn't possible because missing cert
        Assert.assertEquals( 1, exitCode );
    }

    @Test
    public void txPolicyAgainstBackupPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given backup port has ssl
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and backup client is encrypted with cluster ssl
        IntSupplier backupClient = backupClientWithClusterEncryption( OnlineBackupSettings.online_backup_server );

        // when backup against backup port
        int exitCode = backupClient.getAsInt();

        // then
        Assert.assertEquals( 1, exitCode );
    }

    @Test
    public void backupPolicyAgainstBackupPortWithSslPasses() throws IOException, TimeoutException, ExecutionException, InterruptedException
    {
        // given backup port has ssl
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and backup client is encrypted with cluster ssl
        IntSupplier backupClient = backupClientWithBackupEncryption( OnlineBackupSettings.online_backup_server );

        // when backup against backup port
        int exitCode = backupClient.getAsInt();

        // then
        Assert.assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void noPolicyAgainstTxPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given backup port has ssl
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, false );
        cluster.start();
        createSomeData( cluster );

        // and backup client is encrypted with cluster ssl
        IntSupplier backupClient = backupClientWithoutEncryption( CausalClusteringSettings.transaction_listen_address );

        // when backup against backup port
        int exitCode = backupClient.getAsInt();

        // then
        Assert.assertEquals( 1, exitCode );
    }

    @Test
    public void txPolicyAgainstTxPortWithSslPasses() throws IOException, TimeoutException, ExecutionException, InterruptedException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, false );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithClusterEncryption( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void backupPolicyAgainstTxPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithBackupEncryption( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 1, exitCode );
    }

    @Test
    public void noPolicyAgainstUnencryptedBackupWorks() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, false );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithoutEncryption( OnlineBackupSettings.online_backup_server );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
    }

    @Test
    public void noPolicyAgainstUnencryptedTxPasses() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        cluster = aCluster();
        setupClusterWithEncryption( cluster, false, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClientFunction = backupClientWithoutEncryption( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClientFunction.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void backupPolicyAgainstReplicaTxPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithBackupEncryptionToReplica( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 1, exitCode );
    }

    @Test
    public void backupPolicyAgainstReplicaBackupPortWithSslPasses() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithBackupEncryptionToReplica( OnlineBackupSettings.online_backup_server );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void txPolicyAgainstReplicaBackupPortWithSslFails() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithClusterEncryptionToReplica( OnlineBackupSettings.online_backup_server );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 1, exitCode );
    }

    @Test
    public void txPolicyAgainstReplicaTxPortWithSslPasses() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithClusterEncryptionToReplica( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void noPolicyAgainstReplicaBackupWithoutSslPasses() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, true, false );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithoutEncryptionToReplica( OnlineBackupSettings.online_backup_server );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    @Test
    public void noPolicyAgainstReplicaTxWithoutSslPasses() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // given
        cluster = aCluster();
        setupClusterWithEncryption( cluster, false, true );
        cluster.start();
        createSomeData( cluster );

        // and
        IntSupplier backupClient = backupClientWithoutEncryptionToReplica( CausalClusteringSettings.transaction_listen_address );

        // when
        int exitCode = backupClient.getAsInt();

        // then
        assertEquals( 0, exitCode );
        backupDataMatchesDatabase( cluster, backupHome, backupName );
    }

    private Cluster aCluster()
    {
        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        Cluster cluster =
                new CommercialCluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas, new SslHazelcastDiscoveryServiceFactory(), emptyMap(),
                        emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
        return cluster;
    }

    private IntSupplier backupClientWithoutEncryption( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.empty(), false );
    }

    private IntSupplier backupClientWithClusterEncryption( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.of( 0 ), false );
    }

    private IntSupplier backupClientWithBackupEncryption( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.of( BACKUP_SSL_START ), false );
    }

    private IntSupplier backupClientWithBackupEncryptionToReplica( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.of( BACKUP_SSL_START ), true );
    }

    private IntSupplier backupClientWithClusterEncryptionToReplica( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.of( 0 ), true );
    }

    private IntSupplier backupClientWithoutEncryptionToReplica( Setting addressSetting ) throws IOException, TimeoutException
    {
        return backupClient( addressSetting, Optional.empty(), true );
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
    private IntSupplier backupClient( Setting addressSetting, Optional<Integer> baseSslKeyId, boolean replicaOnly ) throws IOException, TimeoutException
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
                Cluster.dataMatchesEventually( cluster.getMemberWithRole( Role.LEADER ), allMembers( cluster ) );
                return runBackupSameJvm( backupHome, backupName, selectedNodeAddress );
            }
            catch ( CommandFailed e )
            {
                e.printStackTrace( System.err );
                return 1;
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
        for ( ClusterMember clusterMember : allMembers( cluster ) )
        {
            copySslToPolicyTrustedDirectory( backupHome, clusterMember.homeDir(), backupPolicyName, targetPolicyName, "backup-client.crt" );
            copySslToPolicyTrustedDirectory( clusterMember.homeDir(), backupHome, targetPolicyName, backupPolicyName,
                    "from-cluster-" + clusterUniqueServerId( clusterMember ) + ".crt" );
        }
    }

    private void installCryptographicObjectsToBackupHome( File neo4J_home, int keyId ) throws IOException
    {
        createConfigFile( neo4J_home );
        File certificatesLocation = neo4J_home.toPath().resolve( "certificates/" + backupPolicyName ).toFile();
        System.out.printf( "Installing keyId %d to %s\n", keyId, certificatesLocation );
        certificatesLocation.mkdirs();
        installSsl( fsRule, certificatesLocation, keyId );
        copySslToPolicyTrustedDirectory( neo4J_home, neo4J_home, backupPolicyName, "backup-key-copy.crt" );
    }

    private static void createConfigFile( File neo4J_home ) throws IOException
    {
        File config = neo4J_home.toPath().resolve( "conf/neo4j.conf" ).toFile();
        File backupPolicyLocation = neo4J_home.toPath().resolve( "certificates/backup" ).toFile();
        Assert.assertTrue( backupPolicyLocation.mkdirs() );
        List<String> lines = new ArrayList<>();
        SslPolicyConfig backupSslConfigGroup = new SslPolicyConfig( backupPolicyName );
        lines.add( settingAsRaw( OnlineBackupSettings.ssl_policy, backupPolicyName ) );
        lines.add( settingAsRaw( backupSslConfigGroup.base_directory, backupPolicyLocation ) );
        Assert.assertTrue( config.getParentFile().mkdirs() );
        writeToFile( config, lines );
    }

    private static <E> String settingAsRaw( Setting<E> setting, E value )
    {
        return String.format( "%s=%s", setting.name(), value );
    }

    private static void writeToFile( File file, List<String> lines ) throws IOException
    {
        String lineSeparator = System.getProperty( "line.separator" );
        file.createNewFile();
        try ( FileWriter fileWriter = new FileWriter( file ) )
        {
            for ( String line : lines )
            {
                fileWriter.write( line + lineSeparator );
            }
        }
    }

    private static void installSsl( FileSystemRule fsRule, File baseDir, int keyId ) throws IOException
    {
        fsRule.mkdirs( new File( baseDir, "trusted" ) );
        fsRule.mkdirs( new File( baseDir, "revoked" ) );
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

    private int runBackupSameJvm( File neo4jHome, String backupName, String host ) throws CommandFailed, IncorrectUsage
    {
        return new OnlineBackupCommandBuilder().withSelectedBackupStrategy( SelectedBackupProtocol.CATCHUP ).withHost( host ).backup( neo4jHome, backupName )
               ? 0 : 1;
    }

    private static void backupDataMatchesDatabase( Cluster cluster, File backupDir, String backupName )
    {
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( backupName, backupDir ) );
    }

    // ---------------------- New functionality

    private void setupClusterWithEncryption( Cluster cluster, boolean encryptedTx, boolean encryptedBackup ) throws IOException
    {
        allMembers( cluster ).forEach( member -> member.config().augment( OnlineBackupSettings.online_backup_enabled, "true" ) );
        if ( encryptedTx )
        {
            configureClusterConfigEncryptedCluster( cluster );
            setupEntireClusterTrusted( cluster, "cluster", 0 );
        }
        if ( encryptedBackup )
        {
            configureClusterConfigEncryptedBackup( cluster );
            setupEntireClusterTrusted( cluster, "backup", 6 );
        }
    }

    private void configureClusterConfigEncryptedCluster( Cluster cluster )
    {
        for ( ClusterMember clusterMember : allMembers( cluster ) )
        {
            SslPolicyConfig clusterPolicyConfig = new SslPolicyConfig( clusterPolicyName );
            clusterMember.config().augment( CausalClusteringSettings.ssl_policy, clusterPolicyName );
            clusterMember.config().augment( clusterPolicyConfig.base_directory, "certificates/" + clusterPolicyName );
        }
    }

    private void configureClusterConfigEncryptedBackup( Cluster cluster )
    {
        for ( ClusterMember clusterMember : allMembers( cluster ) )
        {
            SslPolicyConfig backupPolicyConfig = new SslPolicyConfig( backupPolicyName );
            clusterMember.config().augment( OnlineBackupSettings.ssl_policy, backupPolicyName );
            clusterMember.config().augment( backupPolicyConfig.base_directory, "certificates/" + backupPolicyName );
        }
    }

    private static Collection<ClusterMember> allMembers( Cluster cluster )
    {
        Collection<ClusterMember> members = new ArrayList<>();
        members.addAll( cluster.coreMembers() );
        members.addAll( cluster.readReplicas() );
        return members;
    }

    private void setupEntireClusterTrusted( Cluster cluster, String policyName, int baseKey ) throws IOException
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
            return ((CoreClusterMember) clusterMember).serverId();
        }
        int numberOfCores = 3;
        return ((ReadReplica) clusterMember).serverId() + numberOfCores;
    }

    private void prepareCoreToHaveKeys( ClusterMember member, int keyId, String policyName ) throws IOException
    {
        File homeDir = member.homeDir();
        File policyDir = createPolicyDirectories( fsRule, homeDir, policyName );
        createSslInParent( policyDir, keyId );
    }

    private File createPolicyDirectories( FileSystemRule fsRule, File homeDir, String policyName ) throws IOException
    {
        File policyDir = new File( homeDir, "certificates/" + policyName );
        fsRule.mkdirs( new File( policyDir, "trusted" ) );
        fsRule.mkdirs( new File( policyDir, "revoked" ) );
        return policyDir;
    }

    private void createSslInParent( File policyDir, int keyId ) throws IOException
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
        Path sourcePublicKey = Paths.get( sourceHome.getPath(), "certificates", sourcePolicyName, publicKeyName );
        Path targetPublicKey = Paths.get( targetHome.getPath(), "certificates", targetPolicyName, "trusted", targetFileName );
        targetPublicKey.toFile().getParentFile().mkdirs();
        try
        {
            Files.copy( sourcePublicKey, targetPublicKey, StandardCopyOption.REPLACE_EXISTING );
        }
        catch ( NoSuchFileException e )
        {
            throw new RuntimeException( format( "Certificate is missing: %s", sourcePublicKey ), e );
        }
        catch ( RuntimeException e )
        {
            throw new RuntimeException( format( "\nFileA: %s\nFileB: %s\n", sourcePublicKey, targetPublicKey ), e );
        }
    }
}
