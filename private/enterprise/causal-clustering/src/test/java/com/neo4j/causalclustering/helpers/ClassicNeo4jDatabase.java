/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Optional.of;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class ClassicNeo4jDatabase
{
    private final DatabaseLayout databaseLayout;

    private ClassicNeo4jDatabase( DatabaseLayout databaseLayout )
    {
        this.databaseLayout = databaseLayout;
    }

    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
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

        private String databaseName = DEFAULT_DATABASE_NAME;
        private boolean needRecover;
        private int nrOfNodes = 10;
        private String recordFormat = Standard.LATEST_NAME;
        private File transactionLogsRootDirectoryAbsolute;
        private File databasesRootDirectoryAbsolute;

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

        public Neo4jDatabaseBuilder databaseName( String databaseName )
        {
            this.databaseName = databaseName;
            return this;
        }

        public Neo4jDatabaseBuilder needToRecover()
        {
            this.needRecover = true;
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
            File databaseDirectory = new File( databasesRootDirectoryAbsolute, databaseName );
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setFileSystem( fileSystem )
                    .newEmbeddedDatabaseBuilder( databaseDirectory )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, FALSE )
                    .setConfig( GraphDatabaseSettings.transaction_logs_root_path, transactionLogsRootDirectoryAbsolute.getAbsolutePath() )
                    .newGraphDatabase();

            for ( int i = 0; i < (nrOfNodes / 2); i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node1 = db.createNode( Label.label( "Label-" + i ) );
                    Node node2 = db.createNode( Label.label( "Label-" + i ) );
                    node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                    tx.success();
                }
            }

            if ( needRecover )
            {
                fakeUnrecoveredDatabase( db );
            }
            else
            {
                db.shutdown();
            }

            return new ClassicNeo4jDatabase( DatabaseLayout.of( databaseDirectory, () -> of( transactionLogsRootDirectoryAbsolute ) ) );
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

            db.shutdown();

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
