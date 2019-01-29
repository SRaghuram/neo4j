/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.SslSharedDiscoveryServiceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.backup.impl.OnlineBackupCommandCcIT;
import org.neo4j.backup.impl.SelectedBackupProtocol;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryClassExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {SuppressOutputExtension.class, TestDirectoryExtension.class, DefaultFileSystemExtension.class} )
class ClusteredSystemDatabaseBackupRestoreIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;

    private Cluster<SslDiscoveryServiceFactory> cluster;
    private File backupLocation;
    private String backupAddress = OnlineBackupSettings.online_backup_server.name();
    private File clusterLocation;

    @BeforeEach
    void setup() throws InterruptedException, ExecutionException
    {
        backupLocation = testDirectory.directory( "backupLocation" + UUID.randomUUID().toString() );
        clusterLocation = testDirectory.directory( "cluster" + UUID.randomUUID().toString() );
        cluster = createCluster( clusterLocation, getConfigMap() );
        cluster.start();
    }

    @AfterEach
    void tearDown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void backingUpSystemDatabaseShouldBeSuccessful() throws Exception
    {
        String backupName = "system.db-backup";
        CoreClusterMember leader = cluster.awaitLeader();
        String leaderAddress = leader.settingValue( backupAddress );

        assertTrue( runBackupSameJvm( backupLocation, backupName, leaderAddress, GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) );
        assertEquals( OnlineBackupCommandCcIT.getBackupDbRepresentation( backupName, backupLocation ),
                DbRepresentation.of( getSystemDatabase( cluster ) ) );

        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL dbms.security.createUser('newAdmin', 'testPassword', false)" );
            tx.success();
        } );

        assertNotEquals( DbRepresentation.of( getSystemDatabase( cluster ) ),
                OnlineBackupCommandCcIT.getBackupDbRepresentation( backupName, backupLocation ) );
    }

    @Test
    void restoreSystemDatabaseShouldBeSuccessful() throws Exception
    {
        // given
        String preBackupUsername = "preBackup";
        String postBackupUsername = "postBackup";
        String backupName = "system.db-backup";
        CoreClusterMember leader = cluster.awaitLeader();
        String leaderAddress = leader.settingValue( backupAddress );

        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL dbms.security.createUser('" + preBackupUsername + "', 'testPassword', false)" );
            tx.success();
        } );

        assertTrue( runBackupSameJvm( backupLocation, backupName, leaderAddress, GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) );

        cluster.coreTx( ( db, tx ) ->
        {
            db.execute( "CALL dbms.security.createUser('" + postBackupUsername + "', 'testPassword', false)" );
            tx.success();
        } );

        List<Config> memberConfigs = cluster.coreMembers().stream().map( CoreClusterMember::config ).collect( Collectors.toList() );

        cluster.shutdown();

        // when
        for ( Config config : memberConfigs )
        {
            runRestore( fs, backupLocation, backupName, config );
        }

        unbindCluster( cluster, fs );
        cluster.start();

        //then
        cluster.awaitLeader();

        cluster.coreTx( ( db, tx ) ->
        {
            Result securityResults = db.execute( "CALL dbms.security.listUsers() YIELD username" );
            Set<String> systemUsernames = securityResults.stream().map( r -> (String) r.get( "username" ) ).collect( Collectors.toSet() );
            assertTrue( systemUsernames.contains( preBackupUsername ) );
            assertFalse( systemUsernames.contains( postBackupUsername ) );
            tx.success();
        } );
    }

    private void unbindCluster( Cluster<SslDiscoveryServiceFactory> cluster, FileSystemAbstraction fs ) throws IOException
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            fs.deleteRecursively( member.clusterStateDirectory() );
        }

        this.cluster = createCluster( clusterLocation, getConfigMap() );
    }

    private static Cluster<SslDiscoveryServiceFactory> createCluster( File clusterLocation, Map<String,String> configMap )
    {
        return new CommercialCluster( clusterLocation, 3, 0,
                new SslSharedDiscoveryServiceFactory(), configMap, Collections.emptyMap(), configMap,
                Collections.emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
    }

    private static void runRestore( FileSystemAbstraction fs, File backupLocation, String backupName, Config memberConfig ) throws Exception
    {
        Config restoreCommandConfig = Config.defaults();
        restoreCommandConfig.augment( memberConfig );
        restoreCommandConfig.augment( GraphDatabaseSettings.active_database, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        new RestoreDatabaseCommand( fs, new File( backupLocation, backupName ), restoreCommandConfig,
                GraphDatabaseSettings.SYSTEM_DATABASE_NAME, true ).execute();
    }

    private static boolean runBackupSameJvm( File neo4jHome, String backupName, String host, String databaseName ) throws CommandFailed, IncorrectUsage
    {
        return new OnlineBackupCommandBuilder().withSelectedBackupStrategy( SelectedBackupProtocol.CATCHUP )
                .withDatabase( databaseName )
                .withHost( host )
                .backup( neo4jHome, backupName );
    }

    private static GraphDatabaseService getSystemDatabase( Cluster<?> cluster ) throws Exception
    {
        DatabaseManager databaseManager = cluster.awaitLeader().database()
                .getDependencyResolver()
                .resolveDependency( DatabaseManager.class, DependencyResolver.SelectionStrategy.FIRST );

        return databaseManager.getDatabaseFacade( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ).orElseThrow( IllegalStateException::new );
    }

    private static Map<String,String> getConfigMap()
    {
        Map<String,String> configMap = new HashMap<>();
        configMap.put( OnlineBackupSettings.online_backup_enabled.name(), "true" );
        configMap.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
        configMap.put( SecuritySettings.auth_provider.name(), SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        return configMap;
    }
}
