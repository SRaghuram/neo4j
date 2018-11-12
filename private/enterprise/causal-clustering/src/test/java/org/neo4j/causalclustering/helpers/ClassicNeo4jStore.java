/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helpers;

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
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class ClassicNeo4jStore
{
    private final File storeDir;

    private ClassicNeo4jStore( File storeDir )
    {
        this.storeDir = storeDir;
    }

    public File getStoreDir()
    {
        return storeDir;
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
        private String recordsFormat = Standard.LATEST_NAME;
        private final File baseDir;
        private final FileSystemAbstraction fsa;
        private String absoluteLogicalLogsRootLocation = "";

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
            recordsFormat = format;
            return this;
        }

        public Neo4jStoreBuilder logicalLogsRootLocation( String absoluteLogicalLogsRootLocation )
        {
            this.absoluteLogicalLogsRootLocation = absoluteLogicalLogsRootLocation;
            return this;
        }

        public ClassicNeo4jStore build() throws IOException
        {
            File storeDir = new File( baseDir, dbName );
            File logsRootDir = isEmpty( absoluteLogicalLogsRootLocation ) ? baseDir : new File( absoluteLogicalLogsRootLocation );
            createStore( baseDir, fsa, dbName, nrOfNodes, recordsFormat, needRecover, logsRootDir.getAbsolutePath() );
            return new ClassicNeo4jStore( storeDir  );
        }

        private static void createStore( File base, FileSystemAbstraction fileSystem, String dbName, int nodesToCreate, String recordFormat,
                boolean recoveryNeeded, String logicalLogsRoot ) throws IOException
        {
            File storeDir = new File( base, dbName );
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setFileSystem( fileSystem )
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                    .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                    .setConfig( GraphDatabaseSettings.transaction_logs_root_path, logicalLogsRoot )
                    .newGraphDatabase();

            for ( int i = 0; i < (nodesToCreate / 2); i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node1 = db.createNode( Label.label( "Label-" + i ) );
                    Node node2 = db.createNode( Label.label( "Label-" + i ) );
                    node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                    tx.success();
                }
            }

            if ( recoveryNeeded )
            {
                LogFiles logFiles = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogFiles.class );
                File logFilesDirectory = logFiles.logFilesDirectory();
                File tmpLogs = new File( base, "unrecovered" );
                fileSystem.mkdir( tmpLogs );
                for ( File file : logFiles.logFiles() )
                {
                    fileSystem.copyFile( file, new File( tmpLogs, file.getName() ) );
                }

                db.shutdown();

                for ( File file : logFiles.logFiles() )
                {
                    fileSystem.deleteFile( file );
                }

                for ( File file : logFiles.logFiles() )
                {
                    fileSystem.copyToDirectory( file, logFilesDirectory );
                }
            }
            else
            {
                db.shutdown();
            }
        }
    }
}
