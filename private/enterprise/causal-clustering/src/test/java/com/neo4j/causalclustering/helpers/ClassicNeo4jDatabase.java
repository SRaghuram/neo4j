/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.util.Optional.of;
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
    public static Neo4jDatabaseBuilder builder( File baseDir, FileSystemAbstraction fileSystem )
    {
        return new Neo4jDatabaseBuilder( baseDir, fileSystem );
    }

    public DatabaseLayout layout()
    {
        return databaseLayout;
    }

    public static class Neo4jDatabaseBuilder
    {
        private final File baseDirectoryAbsolute;
        private final FileSystemAbstraction fileSystem;

        private DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase();
        private boolean needRecover;
        private boolean transactionLogsInDatabaseFolder;
        private int nrOfNodes = 10;
        private String recordFormat = Standard.LATEST_NAME;
        private File transactionLogsRootDirectoryAbsolute;
        private File databasesRootDirectoryAbsolute;
        private DatabaseManagementService managementService;

        Neo4jDatabaseBuilder( File baseDirectoryAbsolute, FileSystemAbstraction fileSystem )
        {
            assertAbsolute( baseDirectoryAbsolute );

            this.baseDirectoryAbsolute = baseDirectoryAbsolute;
            this.transactionLogsRootDirectoryAbsolute = new File( baseDirectoryAbsolute, DEFAULT_TX_LOGS_ROOT_DIR_NAME );
            this.databasesRootDirectoryAbsolute = new File( baseDirectoryAbsolute, DEFAULT_DATABASES_ROOT_DIR_NAME );
            this.fileSystem = fileSystem;
        }

        private void assertAbsolute( File directory )
        {
            if ( !directory.isAbsolute() )
            {
                throw new IllegalArgumentException( "Path must be absolute" );
            }
        }

        public Neo4jDatabaseBuilder databaseId( DatabaseId databaseId )
        {
            this.databaseId = databaseId;
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

        public Neo4jDatabaseBuilder transactionLogsRootDirectory( File transactionLogsRootDirectoryAbsolute )
        {
            assertAbsolute( transactionLogsRootDirectoryAbsolute );
            this.transactionLogsRootDirectoryAbsolute = transactionLogsRootDirectoryAbsolute;
            return this;
        }

        public ClassicNeo4jDatabase build() throws IOException
        {
            File databaseDirectory = new File( databasesRootDirectoryAbsolute, databaseId.name() );
            managementService = new TestDatabaseManagementServiceBuilder( databasesRootDirectoryAbsolute )
                    .setFileSystem( fileSystem )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                    .setConfig( GraphDatabaseSettings.transaction_logs_root_path, getTransactionLogsRoot() )
                    .build();
            GraphDatabaseService db = managementService.database( databaseId.name() );

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

            return new ClassicNeo4jDatabase( DatabaseLayout.of( databaseDirectory, () -> of( transactionLogsRootDirectoryAbsolute ) ) );
        }

        private Path getTransactionLogsRoot()
        {
            File directory = transactionLogsInDatabaseFolder ? databasesRootDirectoryAbsolute : transactionLogsRootDirectoryAbsolute;
            return directory.toPath().toAbsolutePath();
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
            File logFilesDirectory = logFiles.logFilesDirectory();

            /* Copy live transaction logs to temporary directory. */
            File temporaryDirectory = new File( baseDirectoryAbsolute, "temp" );
            fileSystem.mkdirs( temporaryDirectory );
            for ( File file : logFiles.logFiles() )
            {
                fileSystem.copyFile( file, new File( temporaryDirectory, file.getName() ) );
            }

            managementService.shutdown();

            /* Delete proper transaction logs. */
            for ( File file : logFiles.logFiles() )
            {
                fileSystem.deleteFileOrThrow( file );
            }

            /* Restore the previously copied transaction logs. */
            LogFiles copyLogFiles = logFilesBasedOnlyBuilder( temporaryDirectory, fileSystem ).build();
            for ( File file : copyLogFiles.logFiles() )
            {
                fileSystem.copyToDirectory( file, logFilesDirectory );
            }
        }
    }
}
