/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helpers;

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
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.TestGraphDatabaseFactory;

public class ClassicNeo4jDatabase
{
    private final DatabaseLayout databaseLayout;

    private ClassicNeo4jDatabase( File databaseDirectory )
    {
        this.databaseLayout = DatabaseLayout.of( databaseDirectory );
    }

    public DatabaseLayout layout()
    {
        return databaseLayout;
    }

    public static Neo4jStoreBuilder builder( File baseDir, FileSystemAbstraction fsa )
    {
        return new Neo4jStoreBuilder( baseDir, fsa );
    }

    public static class Neo4jStoreBuilder
    {
        private String dbName = "graph.db";
        private boolean needRecover;
        private int nrOfNodes = 10;
        private String recordFormat = Standard.LATEST_NAME;
        private final File baseDir;
        private final FileSystemAbstraction fsa;
        private String logicalLogsLocation = "";

        Neo4jStoreBuilder( File baseDir, FileSystemAbstraction fsa )
        {
            this.baseDir = baseDir;
            this.fsa = fsa;
        }

        public Neo4jStoreBuilder dbName( String string )
        {
            dbName = string;
            return this;
        }

        public Neo4jStoreBuilder needToRecover()
        {
            needRecover = true;
            return this;
        }

        public Neo4jStoreBuilder amountOfNodes( int nodes )
        {
            nrOfNodes = nodes;
            return this;
        }

        public Neo4jStoreBuilder recordFormats( String format )
        {
            recordFormat = format;
            return this;
        }

        public Neo4jStoreBuilder logicalLogsLocation( String logicalLogsLocation )
        {
            this.logicalLogsLocation = logicalLogsLocation;
            return this;
        }

        public ClassicNeo4jDatabase build() throws IOException
        {
            createStore();
            return new ClassicNeo4jDatabase( new File( baseDir, dbName ) );
        }

        private void createStore() throws IOException
        {
            File storeDir = new File( baseDir, dbName );
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setFileSystem( fsa )
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                    .setConfig( GraphDatabaseSettings.logical_logs_location, logicalLogsLocation )
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
                File tmpLogs = new File( baseDir, "unrecovered" );
                fsa.mkdir( tmpLogs );
                File txLogsDir = new File( storeDir, logicalLogsLocation );
                for ( File file : fsa.listFiles( txLogsDir, TransactionLogFiles.DEFAULT_FILENAME_FILTER ) )
                {
                    fsa.copyFile( file, new File( tmpLogs, file.getName() ) );
                }

                db.shutdown();

                for ( File file : fsa.listFiles( txLogsDir, TransactionLogFiles.DEFAULT_FILENAME_FILTER ) )
                {
                    fsa.deleteFile( file );
                }

                for ( File file : fsa.listFiles( tmpLogs, TransactionLogFiles.DEFAULT_FILENAME_FILTER ) )
                {
                    fsa.copyFile( file, new File( txLogsDir, file.getName() ) );
                }
            }
            else
            {
                db.shutdown();
            }
        }
    }
}
