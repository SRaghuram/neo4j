/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.restore;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.LayoutConfig.of;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class RestoreDatabaseCommandIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void forceShouldRespectStoreLock()
    {
        var databaseId = new DatabaseId( "to" );
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        DatabaseLayout toLayout = directory.databaseLayout( databaseId.name() );
        int fromNodeCount = 10;
        int toNodeCount = 20;

        createDbAt( fromPath, fromNodeCount );
        createDbAt( toLayout.databaseDirectory(), toNodeCount );

        CommandFailed commandFailedException = assertThrows( CommandFailed.class, () ->
        {
            try ( StoreLocker storeLocker = new StoreLocker( fileSystem, toLayout.getStoreLayout() ) )
            {
                storeLocker.checkLock();
                new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseId, true ).execute();
            }
        } );
        assertThat( commandFailedException.getMessage(), equalTo( "the database is in use -- stop Neo4j and try again" ) );
    }

    @Test
    void shouldNotCopyOverAndExistingDatabase() throws Exception
    {
        // given
        var databaseId = new DatabaseId( "to" );
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        DatabaseLayout toLayout = directory.databaseLayout( databaseId.name() );

        createDbAt( fromPath, 0 );
        createDbAt( toLayout.databaseDirectory(), 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseId, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Database with name [to] already exists" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotExist() throws Exception
    {
        // given
        var databaseId = new DatabaseId( "to" );
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        DatabaseLayout toLayout = directory.databaseLayout( databaseId.name() );

        createDbAt( toLayout.databaseDirectory(), 0 );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseId, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory does not exist" ), illegalException.getMessage() );
    }

    @Test
    void shouldThrowExceptionIfBackupDirectoryDoesNotHaveStoreFiles()
    {
        // given
        var databaseId = new DatabaseId( "to" );
        Config config = configWith( directory.absolutePath().getAbsolutePath() );

        File fromPath = new File( directory.absolutePath(), "from" );
        assertTrue( fromPath.mkdirs() );

        IllegalArgumentException illegalException =
                assertThrows( IllegalArgumentException.class, () -> new RestoreDatabaseCommand( fileSystem, fromPath, config, databaseId, false ).execute() );
        assertTrue( illegalException.getMessage().contains( "Source directory is not a database backup" ), illegalException.getMessage() );
    }

    @Test
    void shouldAllowForcedCopyOverAnExistingDatabase() throws Exception
    {
        // given
        File toStoreDirectory = directory.storeDir( "to" );
        File fromStoreDirectory = directory.storeDir( "from" );
        Config config = configWith( toStoreDirectory.getAbsolutePath() );

        DatabaseLayout fromLayout = directory.databaseLayout( fromStoreDirectory );
        DatabaseLayout toLayout = directory.databaseLayout( toStoreDirectory );
        int fromNodeCount = 10;
        int toNodeCount = 20;
        config.augment( transaction_logs_root_path, toLayout.getTransactionLogsDirectory().getParentFile().getAbsolutePath() );

        createDbAt( fromLayout.databaseDirectory(), fromNodeCount );
        createDbAt( toLayout, toNodeCount );

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, new DatabaseId( DEFAULT_DATABASE_NAME ), true ).execute();

        // then
        DatabaseManagementService managementService = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( toLayout.databaseDirectory() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).newDatabaseManagementService();
        GraphDatabaseService copiedDb = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction ignored = copiedDb.beginTx() )
        {
            assertEquals( fromNodeCount, Iterables.count( copiedDb.getAllNodes() ) );
        }

        copiedDb.shutdown();
    }

    @Test
    void restoreTransactionLogsInCustomDirectoryForTargetDatabaseWhenConfigured()
            throws IOException, CommandFailed
    {
        File toStoreDirectory = directory.storeDir( "to" );
        File fromStoreDirectory = directory.storeDir( "from" );
        Config config = configWith( directory.absolutePath().getAbsolutePath() );
        File customTxLogDirectory = directory.directory( "customLogicalLog" );
        String customTransactionLogDirectory = customTxLogDirectory.getAbsolutePath();
        config.augmentDefaults( transaction_logs_root_path, customTransactionLogDirectory );

        DatabaseLayout fromLayout = directory.databaseLayout( fromStoreDirectory );
        DatabaseLayout toLayout = directory.databaseLayout( toStoreDirectory, of( config ) );
        int fromNodeCount = 10;
        int toNodeCount = 20;
        createDbAt( fromLayout.databaseDirectory(), fromNodeCount );

        GraphDatabaseService db = createDatabase( toLayout.databaseDirectory(), customTxLogDirectory );
        createTestData( toNodeCount, db );
        db.shutdown();

        // when
        new RestoreDatabaseCommand( fileSystem, fromLayout.databaseDirectory(), config, new DatabaseId( DEFAULT_DATABASE_NAME ), true ).execute();

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
    void doNotRemoveRelativeTransactionDirectoryAgain() throws IOException, CommandFailed
    {
        FileSystemAbstraction fs = Mockito.spy( fileSystem );
        File fromPath = directory.directory( "from" );
        DatabaseLayout testLayout = directory.databaseLayout("testDatabase");
        File relativeLogDirectory = directory.directory( "relativeDirectory" );

        Config config = configWith( directory.absolutePath().getAbsolutePath() );
        config.augment( transaction_logs_root_path, relativeLogDirectory.getAbsolutePath() );

        createDbAt( fromPath, 10 );

        new RestoreDatabaseCommand( fs, fromPath, config, new DatabaseId( "testDatabase" ), true ).execute();

        verify( fs ).deleteRecursively( eq( testLayout.databaseDirectory() ) );
        verify( fs, never() ).deleteRecursively( eq( relativeLogDirectory ) );
    }

    @Test
    void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new RestoreDatabaseCliProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin restore --from=<backup-directory> [--database=<name>]%n" +
                            "                           [--force[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Restore a backed up database.%n" +
                            "%n" +
                            "options:%n" +
                            "  --from=<backup-directory>   Path to backup to restore from.%n" +
                            "  --database=<name>           Name of database. [default:neo4j]%n" +
                            "  --force=<true|false>        If an existing database should be replaced.%n" +
                            "                              [default:false]%n" ),
                    baos.toString() );
        }
    }

    private static Config configWith( String dataDirectory )
    {
        return Config.defaults( stringMap( GraphDatabaseSettings.databases_root_path.name(), dataDirectory ) );
    }

    private void createDbAt( File fromPath, int nodesToCreate )
    {
        GraphDatabaseService db = createDatabase( fromPath );
        createTestData( nodesToCreate, db );
        db.shutdown();
    }

    private void createDbAt( DatabaseLayout toLayout, int toNodeCount )
    {
        GraphDatabaseService db = createDatabase( toLayout.databaseDirectory(), toLayout.getTransactionLogsDirectory().getParentFile() );
        createTestData( toNodeCount, db );
        db.shutdown();
    }

    private GraphDatabaseService createDatabase( File path )
    {
        DatabaseManagementService managementService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( transaction_logs_root_path, path.getParentFile().getAbsolutePath() ).newDatabaseManagementService();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseService createDatabase( File path, File transactionRootLocation )
    {
        DatabaseManagementService managementService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( transaction_logs_root_path, transactionRootLocation.getAbsolutePath() ).newDatabaseManagementService();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void createTestData( int nodesToCreate, GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodesToCreate; i++ )
            {
                db.createNode();
            }
            tx.success();
        }
    }
}
