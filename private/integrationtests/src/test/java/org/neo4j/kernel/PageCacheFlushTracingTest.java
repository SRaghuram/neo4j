/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class PageCacheFlushTracingTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void tracePageCacheFlushProgress()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                                            .newEmbeddedDatabaseBuilder( testDirectory.directory() )
                                            .setConfig( GraphDatabaseSettings.tracer, "verbose" )
                                            .newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
        database.shutdown();
        logProvider.assertContainsMessageContaining( "Flushing file" );
        logProvider.assertContainsMessageContaining( "Page cache flush completed. Flushed " );
    }
}
