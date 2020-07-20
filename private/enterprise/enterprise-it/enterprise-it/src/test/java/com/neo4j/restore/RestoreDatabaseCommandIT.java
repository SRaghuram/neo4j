/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

@Neo4jLayoutExtension
class RestoreDatabaseCommandIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private Neo4jLayout neo4jLayout;
    private static DatabaseManagementService managementService;

    @Test
    void forceShouldRespectDatabaseLock()
    {
        var databaseName =  "new" ;
        Neo4jLayout testStore = neo4jLayout;
        Config config = configWith( testStore );

        Path fromPath = directory.homePath().resolve( "old" );

        DatabaseLayout toLayout = testStore.databaseLayout( databaseName );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromPath, fromNodeCount );
        createDbAt( toLayout, toNodeCount );

        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class, () ->
        {
            try ( Locker storeLocker = new DatabaseLocker( fileSystem, toLayout ) )
            {
                storeLocker.checkLock();
                new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, true, false ).execute();
            }
        } );
        assertThat( commandFailedException.getMessage() ).isEqualTo( "The database is in use. Stop database 'new' and try again." );
    }

    @Test
    void shouldNotCopyOverAnExistingDatabase()
    {
        // given
        var databaseName =  "new" ;
        Neo4jLayout testStore = neo4jLayout;
        Config config = configWith( testStore );

        Path fromPath = directory.homePath().resolve( "old" );
        DatabaseLayout toLayout = testStore.databaseLayout( databaseName );

        createDbAt( fromPath, 0 );
        createDbAt( toLayout, 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class,
                              () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Database with name [new] already exists" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotExist()
    {
        // given
        var databaseName =  "new" ;
        Config config = configWith( neo4jLayout );

        Path fromPath = directory.homePath().resolve( "old" );
        DatabaseLayout toLayout = neo4jLayout.databaseLayout( databaseName );

        createDbAt( toLayout.databaseDirectory(), 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class,
                              () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory does not exist" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotHaveStoreFiles() throws IOException
    {
        // given
        var databaseName =  "new" ;
        Config config = configWith( neo4jLayout );

        Path fromPath = directory.homePath().resolve( "old" );
        Files.createDirectories( fromPath );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class,
                              () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory is not a database backup" ), illegalException.getMessage() );
    }

    @Test
    void failOnInvalidDatabaseName() throws IOException
    {
        var databaseName =  "__any_" ;
        Config config = configWith( neo4jLayout );

        Path fromPath = directory.homePath().resolve( "old" );
        Files.createDirectories( fromPath );
        assertThrows( Exception.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false, false ).execute() );
    }

    @Test
    void shouldAllowForcedCopyOverAnExistingDatabase() throws Exception
    {
        // given
        Neo4jLayout toStoreLayout = neo4jLayout;
        Neo4jLayout fromStoreLayout = Neo4jLayout.ofFlat( directory.homePath( "old" ) );

        DatabaseLayout fromLayout = fromStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout toLayout = toStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        Config config = configWith( toStoreLayout );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromLayout, fromNodeCount );
        createDbAt( toLayout, toNodeCount );

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, DEFAULT_DATABASE_NAME, true, false ).execute();

        // then
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( toStoreLayout.homeDirectory() )
                        .setConfig( online_backup_enabled, false )
                        .build();
        GraphDatabaseService copiedDb = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction transaction = copiedDb.beginTx() )
        {
            assertEquals( fromNodeCount, Iterables.count( transaction.getAllNodes() ) );
        }

        managementService.shutdown();
    }

    @Test
    void moveOptionShouldLeaveNoFilesAtSourceAndAWorkingDatabaseInTarget() throws Exception
    {
        // given
        Neo4jLayout toStoreLayout = Neo4jLayout.ofFlat( directory.homePath( "new" ) );
        Neo4jLayout fromStoreLayout = Neo4jLayout.ofFlat( directory.homePath( "old" ) );

        DatabaseLayout fromLayout = fromStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        Config config = configWith( toStoreLayout );
        int fromNodeCount = 10;

        createDbAt( fromLayout, fromNodeCount );

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, DEFAULT_DATABASE_NAME, false, true ).execute();

        // then
        assertThat( fileSystem.fileExists( fromLayout.databaseDirectory().toFile() ) ).isFalse();

        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder( toStoreLayout.homeDirectory() )
                        .setConfig( online_backup_enabled, false )
                        .build();
        GraphDatabaseService copiedDb = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction transaction = copiedDb.beginTx() )
        {
            assertEquals( fromNodeCount, Iterables.count( transaction.getAllNodes() ) );
        }

        managementService.shutdown();
    }

    @Test
    void restoreTransactionLogsInCustomDirectoryForTargetDatabaseWhenConfigured()
            throws IOException
    {
        Neo4jLayout fromStoreLayout = Neo4jLayout.ofFlat( directory.homePath( "old" ) );

        Path newHome = directory.homePath( "new" );
        Config config = Config.newBuilder()
                .set( neo4j_home, newHome )
                .set( transaction_logs_root_path, newHome.resolve( "customLogicalLog" ) )
                .build();

        Neo4jLayout toStoreLayout = Neo4jLayout.of( config );

        DatabaseLayout fromLayout = fromStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout toLayout = toStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        int fromNodeCount = 10;
        int toNodeCount = 20;
        createDbAt( fromLayout, fromNodeCount );

        GraphDatabaseService db = createDatabase( toLayout );
        createTestData( toNodeCount, db );
        StorageEngineFactory storageEngineFactory = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        managementService.shutdown();

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, DEFAULT_DATABASE_NAME, true, false ).execute();

        LogFiles fromStoreLogFiles = logFilesBasedOnlyBuilder( fromLayout.databaseDirectory(), fileSystem )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .build();
        LogFiles toStoreLogFiles = logFilesBasedOnlyBuilder( toLayout.databaseDirectory(), fileSystem )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .build();
        LogFiles customLogLocationLogFiles = logFilesBasedOnlyBuilder( toLayout.getTransactionLogsDirectory(), fileSystem )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .build();
        assertThat( toStoreLogFiles.logFiles() ).isEmpty();
        assertThat( customLogLocationLogFiles.logFiles() ).hasSize( 1 );
        LogFile logFile = fromStoreLogFiles.getLogFile();
        assertThat( Files.size( logFile.getLogFileForVersion( 0 ) ) ).isGreaterThan( 0L );
        assertEquals( Files.size( logFile.getLogFileForVersion( 0 ) ), Files.size( customLogLocationLogFiles.getLogFile().getLogFileForVersion( 0 ) ) );
    }

    @Test
    void shouldDeleteTargetTransactionLogDirectoryWhenItIsSameAsDatabaseDirectory() throws IOException
    {
        String databaseName = "target-database";
        FileSystemAbstraction fs = Mockito.spy( fileSystem );
        Path fromPath = directory.directoryPath( "database-to-restore" );

        Config config = configWith( neo4jLayout );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true, false ).execute();

        verify( fs, never() ).deleteRecursively( neo4jLayout.transactionLogsRootDirectory().toFile() );
    }

    @Test
    void shouldDeleteTargetTransactionLogDirectoryWhenItIsDifferentFromDatabaseDirectory() throws IOException
    {
        String databaseName = "target-database";
        FileSystemAbstraction fs = Mockito.spy( fileSystem );
        Path fromPath = directory.directoryPath( "database-to-restore" );

        Config config = configWith( neo4jLayout );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true, false ).execute();

        verify( fs ).deleteRecursively( neo4jLayout.databaseLayout( databaseName ).getTransactionLogsDirectory().toFile() );
    }

    @Test
    void failIfHasClusterStateForDatabase() throws IOException
    {
        var databaseName =  "new" ;
        Neo4jLayout testStore = neo4jLayout;
        Config config = configWith( testStore );

        Path fromPath = directory.homePath().resolve( "old" );

        createDbAt( fromPath, 10 );
        createRaftGroupDirectoryFor( config, databaseName );

        IllegalArgumentException illegalArgumentException = assertThrows(
                IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, true, false ).execute() );

        assertThat( illegalArgumentException.getMessage() ).isEqualTo(
                "Database with name [new] already exists locally. " +
                "Please run `DROP DATABASE new` against the system database. " +
                "If the database already is dropped, then you need to unbind the local instance using `neo4j-admin unbind`. " +
                "Note that unbind requires stopping the instance, and affects all databases."
        );
    }

    private static Config configWith( Neo4jLayout layout )
    {
        return Config.defaults( neo4j_home, layout.homeDirectory() );
    }

    private void createDbAt( Path fromPath, int nodesToCreate )
    {
        GraphDatabaseService db = createDatabase( DatabaseLayout.ofFlat( fromPath ) );
        createTestData( nodesToCreate, db );
        managementService.shutdown();
    }

    private void createDbAt( DatabaseLayout toLayout, int toNodeCount )
    {
        GraphDatabaseService db = createDatabase( toLayout );
        createTestData( toNodeCount, db );
        managementService.shutdown();
    }

    private static GraphDatabaseService createDatabase( DatabaseLayout databaseLayout )
    {
        Neo4jLayout neo4jLayout = databaseLayout.getNeo4jLayout();
        managementService = new TestDatabaseManagementServiceBuilder( neo4jLayout.homeDirectory() )
                .setConfig( databases_root_path, neo4jLayout.databasesDirectory() )
                .setConfig( transaction_logs_root_path, neo4jLayout.transactionLogsRootDirectory() )
                .setConfig( online_backup_enabled, false )
                .setConfig( default_database, databaseLayout.getDatabaseName() )
                .build();
        return managementService.database( databaseLayout.getDatabaseName() );
    }

    private static void createTestData( int nodesToCreate, GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodesToCreate; i++ )
            {
                tx.createNode();
            }
            tx.commit();
        }
    }

    private void createRaftGroupDirectoryFor( Config config, String databaseName ) throws IOException
    {
        Path raftGroupDir = ClusterStateLayout.of( config.get( GraphDatabaseSettings.data_directory ) ).raftGroupDir( databaseName );
        fileSystem.mkdirs( raftGroupDir.toFile() );
    }
}
