/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

public class ClassicNeo4jDatabase
{
    private final File databaseDirectory;

    private ClassicNeo4jDatabase( File databaseDirectory )
    {
        this.databaseDirectory = databaseDirectory;
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public static Neo4jDatabaseBuilder builder( File baseDir, FileSystemAbstraction fsa )
    {
        return new Neo4jDatabaseBuilder( baseDir, fsa );
    }

    public DatabaseLayout layout()
    {
        return DatabaseLayout.of( databaseDirectory );
    }

    public static class Neo4jDatabaseBuilder
    {
        private final File baseDirectoryAbsolute;
        private final FileSystemAbstraction fsa;

        private String dbName = "graph.db";
        private boolean needRecover;
        private int nrOfNodes = 10;
        private String recordsFormat = Standard.LATEST_NAME;
        private File transactionLogsRootDirectoryAbsolute;

        Neo4jDatabaseBuilder( File baseDirectoryAbsolute, FileSystemAbstraction fsa )
        {
            assertAbsolute( baseDirectoryAbsolute );

            this.baseDirectoryAbsolute = baseDirectoryAbsolute;
            this.transactionLogsRootDirectoryAbsolute = baseDirectoryAbsolute;
            this.fsa = fsa;
        }

        private void assertAbsolute( File directory )
        {
            if ( !directory.isAbsolute() )
            {
                throw new IllegalArgumentException( "Path must be absolute" );
            }
        }

        public Neo4jDatabaseBuilder dbName( String dbName )
        {
            this.dbName = dbName;
            return this;
        }

        public Neo4jDatabaseBuilder needToRecover()
        {
            needRecover = true;
            return this;
        }

        public Neo4jDatabaseBuilder amountOfNodes( int nodes )
        {
            nrOfNodes = nodes;
            return this;
        }

        public Neo4jDatabaseBuilder recordFormats( String format )
        {
            recordsFormat = format;
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
            File databaseDirectory = new File( baseDirectoryAbsolute, dbName );
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setFileSystem( fsa )
                    .newEmbeddedDatabaseBuilder( databaseDirectory )
                    .setConfig( GraphDatabaseSettings.record_format, recordsFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
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
                LogFiles logFiles = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogFiles.class );
                File logFilesDirectory = logFiles.logFilesDirectory();
                File tmpLogs = new File( baseDirectoryAbsolute, "unrecovered" );
                fsa.mkdir( tmpLogs );
                for ( File file : logFiles.logFiles() )
                {
                    fsa.copyFile( file, new File( tmpLogs, file.getName() ) );
                }

                db.shutdown();

                for ( File file : logFiles.logFiles() )
                {
                    fsa.deleteFile( file );
                }

                LogFiles copyLogFiles = logFilesBasedOnlyBuilder( tmpLogs, fsa ).build();
                for ( File file : copyLogFiles.logFiles() )
                {
                    fsa.copyToDirectory( file, logFilesDirectory );
                }
            }
            else
            {
                db.shutdown();
            }
            return new ClassicNeo4jDatabase( databaseDirectory );
        }
    }
}
