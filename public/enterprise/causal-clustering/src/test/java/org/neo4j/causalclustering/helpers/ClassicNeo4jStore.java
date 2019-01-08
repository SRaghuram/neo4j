/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.test.TestGraphDatabaseFactory;

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

        public ClassicNeo4jStore build() throws IOException
        {
            createStore( baseDir, fsa, dbName, nrOfNodes, recordsFormat, needRecover );
            File storeDir = new File( baseDir, dbName );
            return new ClassicNeo4jStore( storeDir );
        }

        private static void createStore( File base, FileSystemAbstraction fileSystem, String dbName, int nodesToCreate, String recordFormat, boolean recoveryNeeded )
                throws IOException
        {
            File storeDir = new File( base, dbName );
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .setFileSystem( fileSystem )
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( GraphDatabaseSettings.record_format, recordFormat )
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
                File tmpLogs = new File( base, "unrecovered" );
                fileSystem.mkdir( tmpLogs );
                for ( File file : fileSystem.listFiles( storeDir, LogFiles.FILENAME_FILTER ) )
                {
                    fileSystem.copyFile( file, new File( tmpLogs, file.getName() ) );
                }

                db.shutdown();

                for ( File file : fileSystem.listFiles( storeDir, LogFiles.FILENAME_FILTER ) )
                {
                    fileSystem.deleteFile( file );
                }
                LogFiles.move( fileSystem, tmpLogs, storeDir );
            }
            else
            {
                db.shutdown();
            }
        }
    }
}
