/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.index;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

public class StartOnExistingDbWithIndexIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void startStopDatabaseWithIndex()
    {
        Label label = Label.label( "string" );
        String property = "property";
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = prepareDb( label, property, logProvider );
        db.shutdown();
        db = getDatabase( logProvider );
        db.shutdown();

        logProvider.assertNoMessagesContaining( "Failed to open index" );
    }

    private GraphDatabaseService prepareDb( Label label, String propertyName, LogProvider logProvider )
    {
        GraphDatabaseService db = getDatabase( logProvider );
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyName ).create();
            transaction.success();
        }
        waitIndexes( db );
        return db;
    }

    private GraphDatabaseService getDatabase( LogProvider logProvider )
    {
        return new TestCommercialGraphDatabaseFactory()
                .setInternalLogProvider( logProvider )
                .newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    private static void waitIndexes( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            transaction.success();
        }
    }
}
