/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.BackupExecutionException;
import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.restore.RestoreDatabaseCommand;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.CausalClusteringInternalSettings.experimental_catchup_protocol;
import static com.neo4j.configuration.CausalClusteringSettings.cluster_state_directory;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store;

@TestDirectoryExtension
public class BackupRestoreIT
{
    @Inject
    private TestDirectory testDirectory;
    private Path backupsDir1;
    private Path backupsDir2;
    private Path serverDir1;
    private DatabaseManagementService dbmsService;

    @BeforeEach
    void beforeEach()
    {
        backupsDir1 = testDirectory.homePath( "backups1" );
        backupsDir2 = testDirectory.homePath( "backups2" );
        serverDir1 = testDirectory.homePath( "server1" );
    }

    @AfterEach
    void afterEach()
    {
        if ( dbmsService != null )
        {
            dbmsService.shutdown();
        }
    }

    @Test
    void shouldThrowExceptionWhenDatabaseIdConflicts() throws Exception
    {
        this.dbmsService = createManagementService( serverDir1, getConfig( Neo4jLayout.of( serverDir1 ), true ) );
        final var databaseA = "databasea";
        var dbServiceA = createAndStartDB( dbmsService, databaseA );
        //given node A
        createPersonNode( dbServiceA, "A" );
        //backup node A
        executeBackup( defaultBackupContextBuilder( backupsDir1, dbServiceA, false ) );
        executeBackup( defaultBackupContextBuilder( backupsDir2, dbServiceA, false ) );
        //create node B
        createPersonNode( dbServiceA, "B" );
        //incremental backup B
        executeBackup( defaultBackupContextBuilder( backupsDir2, dbServiceA, false ) );
        //drop database A
        dbmsService.dropDatabase( dbServiceA.databaseName() );
        dbmsService.shutdown();
        //then restore A
        final var restoreTarget = Neo4jLayout.of( serverDir1 ).databaseLayout( databaseA );
        executeRestoreCommand( Path.of( backupsDir1.toAbsolutePath().toString(), databaseA ), restoreTarget, false );
        this.dbmsService = createManagementService( serverDir1, getConfig( Neo4jLayout.of( serverDir1 ), true ) );
        var newDbServiceA = createAndStartDB( dbmsService, databaseA );
        //create node C
        createPersonNode( newDbServiceA, "C" );
        //incremental backup C
        final var exception =
                assertThrows( BackupExecutionException.class, () ->
                        executeBackup( defaultBackupContextBuilder( backupsDir2, newDbServiceA, false ) ) );
        assertThat( exception.getCause() ).hasMessageContaining( "stored on the file system doesn't match with the server one" );
    }

    private GraphDatabaseService createAndStartDB( DatabaseManagementService service, String dbName )
    {
        service.createDatabase( dbName );
        service.startDatabase( dbName );
        return service.database( dbName );
    }

    private DatabaseManagementService createManagementService( Path path, Map<Setting<?>,Object> settings )
    {
        DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( path );
        builder.setConfig( settings );
        return builder.build();
    }

    private Map<Setting<?>,Object> getConfig( Path databaseRootPath, boolean onlineBackupEnabled )
    {
        Map<Setting<?>,Object> settings = new HashMap<>();
        settings.put( neo4j_home, databaseRootPath.toAbsolutePath() );
        settings.put( online_backup_enabled, onlineBackupEnabled );
        settings.put( experimental_catchup_protocol, true );
        return settings;
    }

    private Map<Setting<?>,Object> getConfig( Neo4jLayout neo4jLayout, boolean onlineBackupEnabled )
    {
        Map<Setting<?>,Object> settings = new HashMap<>();
        settings.put( neo4j_home, neo4jLayout.homeDirectory() );
        settings.put( data_directory, neo4jLayout.dataDirectory() );
        settings.put( databases_root_path, neo4jLayout.databasesDirectory() );
        settings.put( transaction_logs_root_path, neo4jLayout.transactionLogsRootDirectory() );
        settings.put( online_backup_enabled, onlineBackupEnabled );
        settings.put( experimental_catchup_protocol, true );
        return settings;
    }

    private OnlineBackupContext.Builder defaultBackupContextBuilder( Path backupFolder, GraphDatabaseService db, boolean fallbackToFullBackup )
    {
        final var address = backupAddress( db );
        return OnlineBackupContext.builder()
                                  .withDatabaseNamePattern( db.databaseName() )
                                  .withAddress( address )
                                  .withBackupDirectory( backupFolder )
                                  .withReportsDirectory( backupFolder )
                                  .withFallbackToFullBackup( fallbackToFullBackup )
                                  .withConfig( Config.defaults( experimental_catchup_protocol, true ) )
                                  .withConsistencyCheckRelationshipTypeScanStore( enable_relationship_type_scan_store.defaultValue() );
    }

    private static SocketAddress backupAddress( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort address = portRegister.getLocalAddress( BACKUP_SERVER_NAME );
        assertNotNull( address, "Backup server address not registered" );
        return new SocketAddress( address.getHost(), address.getPort() );
    }

    private static void executeBackup( OnlineBackupContext.Builder contextBuilder ) throws Exception
    {
        final var log4jLogProvider = new Log4jLogProvider( System.out );
        OnlineBackupExecutor executor = OnlineBackupExecutor.builder()
                                                            .withUserLogProvider( log4jLogProvider )
                                                            .withInternalLogProvider( log4jLogProvider )
                                                            .withMonitors( new Monitors() )
                                                            .withClock( Clocks.nanoClock() )
                                                            .build();
        executor.executeBackups( contextBuilder.build() );
    }

    private static void executeRestoreCommand( Path fromDatabasePath, DatabaseLayout targetLayout, boolean forceOverwrite ) throws IOException
    {
        final var config = configWith( targetLayout.getNeo4jLayout() );
        final var clusterStateLayout = ClusterStateLayout.of( config.get( cluster_state_directory ) );
        final var databaseName = "target-database";
        final var raftGroupDirectory = clusterStateLayout.raftGroupDir( databaseName );
        new RestoreDatabaseCommand( new DefaultFileSystemAbstraction(), new PrintStream( System.out ), fromDatabasePath, targetLayout, raftGroupDirectory,
                                    forceOverwrite, false ).execute();
    }

    private static Config configWith( Neo4jLayout layout )
    {
        return Config.defaults( neo4j_home, layout.homeDirectory() );
    }

    private void createPersonNode( GraphDatabaseService db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) ).setProperty( "Name", name );
            tx.commit();
        }
    }
}
