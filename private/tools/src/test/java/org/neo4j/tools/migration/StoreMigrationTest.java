/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.migration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

public class StoreMigrationTest
{
    @Rule
    public final SuppressOutput mute = SuppressOutput.suppressAll();
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();

    @Before
    public void setUp() throws IOException
    {
        Unzip.unzip( getClass(), "2.3-store.zip", testDir.databaseDir() );
    }

    @Test
    public void storeMigrationToolShouldBeAbleToMigrateOldStore() throws Exception
    {
        StoreMigration.main( new String[]{testDir.databaseDir().getAbsolutePath()} );

        // after migration we can open store and do something
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDir.databaseDir() )
                .setConfig( GraphDatabaseSettings.logs_directory, testDir.directory( "logs" ).getAbsolutePath() )
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
