/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreDatabaseManager;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.restore.RestoreDatabaseCommand;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static com.neo4j.security.SecurityHelpers.newUser;
import static com.neo4j.security.SecurityHelpers.showUsers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@TestDirectoryExtension
@ClusterExtension
@TestInstance( PER_METHOD )
@ExtendWith( SuppressOutputExtension.class )
class ClusteredSystemDatabaseBackupRestoreIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;
    private File backupLocation;

    @BeforeEach
    void setup() throws InterruptedException, ExecutionException
    {
        backupLocation = testDirectory.directory( "backupLocation" + UUID.randomUUID().toString() );
        cluster = createCluster( getConfigMap() );
        cluster.start();
    }

    @Test
    void backingUpSystemDatabaseShouldBeSuccessful() throws Exception
    {
        String databaseName = GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
        CoreClusterMember leader = cluster.awaitLeader();
        String leaderAddress = leader.settingValue( online_backup_listen_address ).toString();

        assertTrue( runBackupSameJvm( backupLocation, leaderAddress, databaseName ) );
        DbRepresentation backupDbRepresentation = DbRepresentation.of( backupLocation, databaseName, Config
                .newBuilder()
                .set( GraphDatabaseSettings.transaction_logs_root_path, backupLocation.toPath().toAbsolutePath() )
                .set( GraphDatabaseInternalSettings.databases_root_path, backupLocation.toPath().toAbsolutePath() )
                .build() );
        assertEquals( DbRepresentation.of( getSystemDatabase( cluster ) ), backupDbRepresentation );

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( tx, "newAdmin", "testPassword" );
            tx.commit();
        } );

        assertNotEquals( DbRepresentation.of( getSystemDatabase( cluster ) ), backupDbRepresentation );
    }

    @Test
    void restoreSystemDatabaseShouldBeSuccessful() throws Exception
    {
        // given
        String databaseName = GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
        String preBackupUsername = "preBackup";
        String postBackupUsername = "postBackup";
        CoreClusterMember leader = cluster.awaitLeader();
        String leaderAddress = leader.settingValue( online_backup_listen_address ).toString();

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( tx, preBackupUsername, "testPassword" );
            tx.commit();
        } );

        assertTrue( runBackupSameJvm( backupLocation, leaderAddress, databaseName ) );

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( tx, postBackupUsername, "testPassword" );
            tx.commit();
        } );

        List<Config> memberConfigs = cluster.coreMembers().stream().map( CoreClusterMember::config ).collect( Collectors.toList() );

        cluster.shutdown();

        // when
        unbindCluster( cluster, fs );

        for ( Config config : memberConfigs )
        {
            runRestore( fs, backupLocation, config );
        }

        cluster.start();

        //then
        cluster.awaitLeader();

        cluster.systemTx( ( db, tx ) ->
        {
            Result securityResults = showUsers( tx );
            Set<String> systemUsernames = securityResults.stream().map( r -> (String) r.get( "user" ) ).collect( Collectors.toSet() );
            assertTrue( systemUsernames.contains( preBackupUsername ) );
            assertFalse( systemUsernames.contains( postBackupUsername ) );
            tx.commit();
        } );
    }

    private void unbindCluster( Cluster cluster, FileSystemAbstraction fs ) throws IOException
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            fs.deleteRecursively( member.clusterStateDirectory() );
        }
    }

    private Cluster createCluster( Map<String,String> configMap )
    {
        return clusterFactory.createCluster( ClusterConfig.clusterConfig().withNumberOfReadReplicas( 0 ).withSharedCoreParams( configMap ) );
    }

    private static void runRestore( FileSystemAbstraction fs, File backupLocation, Config memberConfig ) throws Exception
    {
        Config restoreCommandConfig = Config.newBuilder().fromConfig( memberConfig ).build();
        var databaseName = GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
        new RestoreDatabaseCommand( fs, new File( backupLocation, databaseName ), restoreCommandConfig,
                databaseName, true, false ).execute();
    }

    private static boolean runBackupSameJvm( File neo4jHome, String host, String databaseName )
    {
        int exitCode = BackupTestUtil.runBackupToolFromSameJvm( neo4jHome,
                "--from", host,
                "--database", databaseName,
                "--backup-dir", neo4jHome.toString() );

        return exitCode == 0;
    }

    private static GraphDatabaseService getSystemDatabase( Cluster cluster ) throws Exception
    {
        var databaseManager = cluster.awaitLeader()
                .defaultDatabase()
                .getDependencyResolver()
                .resolveDependency( CoreDatabaseManager.class );

        return databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID )
                .map( DatabaseContext::databaseFacade ).orElseThrow( IllegalStateException::new );
    }

    private static Map<String,String> getConfigMap()
    {
        Map<String,String> configMap = new HashMap<>();
        configMap.put( OnlineBackupSettings.online_backup_enabled.name(), TRUE );
        configMap.put( GraphDatabaseSettings.auth_enabled.name(), TRUE );
        configMap.put( SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME );
        configMap.put( SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME );
        return configMap;
    }
}
