/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import com.neo4j.configuration.OnlineBackupSettings;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class ClassicNeo4jDatabase
{
    private final DatabaseLayout databaseLayout;

    private ClassicNeo4jDatabase( DatabaseLayout databaseLayout )
    {
        this.databaseLayout = databaseLayout;
    }

    /**
     * @param baseDir generally corresponds to $NEO4J_HOME/data/ in tests.
     */
    public static Neo4jDatabaseBuilder builder( Path baseDir, FileSystemAbstraction fileSystem )
    {
        return new Neo4jDatabaseBuilder( baseDir, fileSystem );
    }

    public DatabaseLayout layout()
    {
        return databaseLayout;
    }

    public static class Neo4jDatabaseBuilder
    {
        private final Path baseDirectoryAbsolute;
        private final FileSystemAbstraction fileSystem;

        private NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
        private boolean needRecover;
        private boolean transactionLogsInDatabaseFolder;
        private int nrOfNodes = 10;
        private String recordFormat = Standard.LATEST_NAME;
        private Path transactionLogsRootDirectoryAbsolute;
        private Path databasesRootDirectoryAbsolute;
        private DatabaseManagementService managementService;

        Neo4jDatabaseBuilder( Path baseDirectoryAbsolute, FileSystemAbstraction fileSystem )
        {
            assertAbsolute( baseDirectoryAbsolute );

            this.baseDirectoryAbsolute = baseDirectoryAbsolute;
            this.transactionLogsRootDirectoryAbsolute = baseDirectoryAbsolute.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME );
            this.databasesRootDirectoryAbsolute = baseDirectoryAbsolute.resolve( DEFAULT_DATABASES_ROOT_DIR_NAME );
            this.fileSystem = fileSystem;
        }

        private void assertAbsolute( Path directory )
        {
            if ( !directory.isAbsolute() )
            {
                throw new IllegalArgumentException( "Path must be absolute" );
            }
        }

        public Neo4jDatabaseBuilder databaseId( NamedDatabaseId namedDatabaseId )
        {
            this.namedDatabaseId = namedDatabaseId;
            return this;
        }

        public Neo4jDatabaseBuilder needToRecover()
        {
            this.needRecover = true;
            return this;
        }

        public Neo4jDatabaseBuilder transactionLogsInDatabaseFolder()
        {
            this.transactionLogsInDatabaseFolder = true;
            return this;
        }

        public Neo4jDatabaseBuilder amountOfNodes( int nrOfNodes )
        {
            this.nrOfNodes = nrOfNodes;
            return this;
        }

        public Neo4jDatabaseBuilder recordFormats( String recordFormat )
        {
            this.recordFormat = recordFormat;
            return this;
        }

        public Neo4jDatabaseBuilder transactionLogsRootDirectory( Path transactionLogsRootDirectoryAbsolute )
        {
            assertAbsolute( transactionLogsRootDirectoryAbsolute );
            this.transactionLogsRootDirectoryAbsolute = transactionLogsRootDirectoryAbsolute;
            return this;
        }

        public ClassicNeo4jDatabase build() throws IOException
        {
            managementService = new TestDatabaseManagementServiceBuilder( baseDirectoryAbsolute )
                    .setFileSystem( fileSystem )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                    .setConfig( GraphDatabaseSettings.transaction_logs_root_path, getTransactionLogsRoot() )
                    .setConfig( GraphDatabaseInternalSettings.databases_root_path, databasesRootDirectoryAbsolute )
                    .build();
            GraphDatabaseService db = managementService.database( namedDatabaseId.name() );

            for ( int i = 0; i < (nrOfNodes / 2); i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node1 = tx.createNode( Label.label( "Label-" + i ) );
                    Node node2 = tx.createNode( Label.label( "Label-" + i ) );
                    node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                    tx.commit();
                }
            }

            if ( needRecover )
            {
                fakeUnrecoveredDatabase( db );
            }
            else
            {
                managementService.shutdown();
            }

            return new ClassicNeo4jDatabase( ((GraphDatabaseAPI) db).databaseLayout() );
        }

        private Path getTransactionLogsRoot()
        {
            Path directory = transactionLogsInDatabaseFolder ? databasesRootDirectoryAbsolute : transactionLogsRootDirectoryAbsolute;
            return directory.toAbsolutePath();
        }

        /**
         * This fakes the need of recovery by copying the transaction logs to a temporary directory
         * while the database still is running. The transaction logs should thus in general not
         * have been check-pointed. These copied transaction logs are then re-installed causing
         * the database to appear as if it requires recovery on startup.
         */
        private void fakeUnrecoveredDatabase( GraphDatabaseService db ) throws IOException
        {
            LogFiles logFiles = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogFiles.class );
            Path logFilesDirectory = logFiles.logFilesDirectory();

            /* Copy live transaction logs to temporary directory. */
            Path temporaryDirectory = baseDirectoryAbsolute.resolve( "temp" );
            fileSystem.mkdirs( temporaryDirectory );
            for ( Path file : logFiles.logFiles() )
            {
                fileSystem.copyFile( file, temporaryDirectory.resolve( file.getFileName() ) );
            }
            StorageEngineFactory storageEngineFactory = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( StorageEngineFactory.class );

            managementService.shutdown();

            /* Delete proper transaction logs. */
            for ( Path file : logFiles.logFiles() )
            {
                fileSystem.deleteFileOrThrow( file );
            }

            /* Restore the previously copied transaction logs. */
            LogFiles copyLogFiles = logFilesBasedOnlyBuilder( temporaryDirectory, fileSystem )
                    .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                    .build();
            for ( Path file : copyLogFiles.logFiles() )
            {
                fileSystem.copyToDirectory( file, logFilesDirectory );
            }
        }
    }
}
