/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class StoreMigrationTest
{
    @Inject
    private TestDirectory directory;

    @BeforeEach
    void setUp() throws IOException
    {
        Unzip.unzip( getClass(), "3.4-store.zip", directory.databaseDir() );
    }

    @Test
    void storeMigrationToolShouldBeAbleToMigrateOldStore() throws Exception
    {
        StoreMigration.main( new String[]{directory.databaseDir().getAbsolutePath()} );

        // after migration we can open store and do something
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.databaseDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, directory.directory( "logs" ).getAbsolutePath() )
                .newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
            node.setProperty( "key", "value" );
            transaction.success();
        }
        finally
        {
            database.shutdown();
        }
    }
}
