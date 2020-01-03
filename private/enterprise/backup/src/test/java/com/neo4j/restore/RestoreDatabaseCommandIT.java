/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
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
        Config config = configWith( testStore.databasesDirectory().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "old" );

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
                new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, true ).execute();
            }
        } );
        assertThat( commandFailedException.getMessage(), equalTo( "The database is in use. Stop database 'new' and try again." ) );
    }

    @Test
    void shouldNotCopyOverAndExistingDatabase() throws Exception
    {
        // given
        var databaseName =  "new" ;
        Neo4jLayout testStore = neo4jLayout;
        Config config = configWith( testStore.databasesDirectory().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "old" );
        DatabaseLayout toLayout = testStore.databaseLayout( databaseName );

        createDbAt( fromPath, 0 );
        createDbAt( toLayout, 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Database with name [new] already exists" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotExist() throws Exception
    {
        // given
        var databaseName =  "new" ;
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "old" );
        DatabaseLayout toLayout = neo4jLayout.databaseLayout( databaseName );

        createDbAt( toLayout.databaseDirectory(), 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory does not exist" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotHaveStoreFiles()
    {
        // given
        var databaseName =  "new" ;
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "old" );
        assertTrue( fromPath.mkdirs() );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseName, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory is not a database backup" ), illegalException.getMessage() );
    }

    @Test
    void shouldAllowForcedCopyOverAnExistingDatabase() throws Exception
    {
        // given
        Neo4jLayout toStoreLayout = neo4jLayout;
        Neo4jLayout fromStoreLayout = Neo4jLayout.ofFlat( directory.homeDir( "old" ) );

        DatabaseLayout fromLayout = fromStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        DatabaseLayout toLayout = toStoreLayout.databaseLayout( DEFAULT_DATABASE_NAME );
        Config config =
                configWith( toStoreLayout.databasesDirectory().getAbsolutePath(), toLayout.getTransactionLogsDirectory().getParentFile().getAbsolutePath() );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromLayout, fromNodeCount );
        createDbAt( toLayout, toNodeCount );

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, DEFAULT_DATABASE_NAME, true ).execute();

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
    void restoreTransactionLogsInCustomDirectoryForTargetDatabaseWhenConfigured()
            throws IOException
    {
        Neo4jLayout fromStoreLayout = Neo4jLayout.ofFlat( directory.homeDir( "old" ) );

        Path newHome = directory.homeDir( "new" ).toPath();
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
        managementService.shutdown();

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, DEFAULT_DATABASE_NAME, true ).execute();

        LogFiles fromStoreLogFiles = logFilesBasedOnlyBuilder( fromLayout.databaseDirectory(), fileSystem ).build();
        LogFiles toStoreLogFiles = logFilesBasedOnlyBuilder( toLayout.databaseDirectory(), fileSystem ).build();
        LogFiles customLogLocationLogFiles = logFilesBasedOnlyBuilder( toLayout.getTransactionLogsDirectory(), fileSystem ).build();
        assertThat( toStoreLogFiles.logFiles(), emptyArray() );
        assertThat( customLogLocationLogFiles.logFiles(), arrayWithSize( 1 ) );
        assertThat( fromStoreLogFiles.getLogFileForVersion( 0 ).length(), greaterThan( 0L ) );
        assertEquals( fromStoreLogFiles.getLogFileForVersion( 0 ).length(),
                customLogLocationLogFiles.getLogFileForVersion( 0 ).length() );
    }

    @Test
    void shouldDeleteTargetTransactionLogDirectoryWhenItIsSameAsDatabaseDirectory() throws IOException
    {
        String databaseName = "target-database";
        FileSystemAbstraction fs = Mockito.spy( fileSystem );
        File fromPath = directory.directory( "database-to-restore" );
        File targetDatabaseDirectory = directory.directory( databaseName );
        File targetTransactionLogDirectory = directory.directory( databaseName );

        Config config = configWith( targetDatabaseDirectory.getParent(), targetTransactionLogDirectory.getParent() );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true ).execute();

        verify( fs, never() ).deleteRecursively( targetTransactionLogDirectory );
    }

    @Test
    void shouldDeleteTargetTransactionLogDirectoryWhenItIsDifferentFromDatabaseDirectory() throws IOException
    {
        String databaseName = "target-database";
        FileSystemAbstraction fs = Mockito.spy( fileSystem );
        File fromPath = directory.directory( "database-to-restore" );
        File targetDatabaseDirectory = directory.directory( databaseName );
        File targetTransactionLogDirectory = directory.directory( "different-tx-log-root-directory", databaseName );

        Config config = configWith( targetDatabaseDirectory.getParent(), targetTransactionLogDirectory.getParent() );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fs, fromPath, config, databaseName, true ).execute();

        verify( fs ).deleteRecursively( targetTransactionLogDirectory );
    }

    private static Config configWith( String dataDirectory )
    {
        return Config.defaults( databases_root_path, Path.of( dataDirectory ).toAbsolutePath() );
    }

    private static Config configWith( String dataDirectory, String transactionDir )
    {
        return Config.newBuilder()
                .set( databases_root_path, Path.of( dataDirectory ).toAbsolutePath() )
                .set( transaction_logs_root_path, Path.of( transactionDir ).toAbsolutePath() ).build();
    }

    private void createDbAt( File fromPath, int nodesToCreate )
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
        managementService = new DatabaseManagementServiceBuilder( neo4jLayout.homeDirectory() )
                .setConfig( databases_root_path, neo4jLayout.databasesDirectory().toPath() )
                .setConfig( transaction_logs_root_path, neo4jLayout.transactionLogsRootDirectory().toPath() )
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
}
