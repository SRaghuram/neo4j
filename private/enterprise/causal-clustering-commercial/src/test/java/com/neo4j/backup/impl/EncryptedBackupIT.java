/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.clusterDatabase;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.createSomeData;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.getBackupDbRepresentation;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;

public class EncryptedBackupIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster cluster;

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
        cluster = anEncryptedCluster();
        installCryptographicObjectsToEachCore( cluster, fsRule );
        cluster.start();

        // and backup client is configured
        File NEO4J_HOME = testDir.directory( "NEO4J_HOME" );
        String backupName = "encryptedBackup";
        File backupDir = testDir.directory( "backup-dir" );
        installCryptographicObjectsToBackupHome( backupDir, backupName, cluster.getDbWithRole( Role.LEADER ).serverId() );

        // when a full backup is successful
        int exitCode = runBackupToolFromOtherJvmToGetExitCode(
                NEO4J_HOME, "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir, "--name=" + backupName,
                "--from=" + backupAddress( cluster ) );
        assertEquals( 0, exitCode );

        // then data matches
        backupDataMatchesDatabase( cluster, backupDir, backupName );

        // when the cluster is populated with more data
        createSomeData( cluster );
        Cluster.dataMatchesEventually( cluster.getDbWithRole( Role.LEADER ), cluster.coreMembers() );

        // then an incremental backup is successful on that cluster
        exitCode = runBackupToolFromOtherJvmToGetExitCode(
                NEO4J_HOME, "--cc-report-dir=" + backupDir,
                "--backup-dir=" + backupDir, "--name=" + backupName,
                "--from=" + backupAddress( cluster ) );
        assertEquals( 0,
                exitCode );

        // and data matches
        backupDataMatchesDatabase( cluster, backupDir, backupName );
    }

    private static String backupAddress( Cluster cluster )
    {
        return cluster.getDbWithRole( Role.LEADER ).settingValue( CausalClusteringSettings.transaction_listen_address.name() );
    }

    private void installCryptographicObjectsToBackupHome( File neo4J_home, String backupName, int keyId ) throws IOException
    {
        File baseDir = new File( neo4J_home, backupName );
        installSsl( fsRule, baseDir, keyId );
    }

    private Cluster anEncryptedCluster()
    {
        String sslPolicyName = "cluster";
        SslPolicyConfig policyConfig = new SslPolicyConfig( sslPolicyName );

        Map<String,String> coreParams =
                stringMap( CausalClusteringSettings.ssl_policy.name(), sslPolicyName, policyConfig.base_directory.name(), "certificates/cluster" );
        Map<String,String> readReplicaParams =
                stringMap( CausalClusteringSettings.ssl_policy.name(), sslPolicyName, policyConfig.base_directory.name(), "certificates/cluster" );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 0;

        return new Cluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas, new SslHazelcastDiscoveryServiceFactory(), coreParams, emptyMap(),
                readReplicaParams, emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
    }

    private static void installCryptographicObjectsToEachCore( Cluster cluster, FileSystemRule fsRule ) throws IOException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            File homeDir = cluster.getCoreMemberById( core.serverId() ).homeDir();
            File baseDir = new File( homeDir, "certificates/cluster" );

            int keyId = core.serverId();
            installSsl( fsRule, baseDir, keyId );
        }
    }

    private static void installSsl( FileSystemRule fsRule, File baseDir, int keyId ) throws IOException
    {
        fsRule.mkdirs( new File( baseDir, "trusted" ) );
        fsRule.mkdirs( new File( baseDir, "revoked" ) );
        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( baseDir );
    }

    private static void backupDataMatchesDatabase( Cluster cluster, File backupDir, String backupName )
    {
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( backupName, backupDir ) );
    }
}
