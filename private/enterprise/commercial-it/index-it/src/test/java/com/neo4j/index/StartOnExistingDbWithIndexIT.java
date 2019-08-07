/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.index;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class StartOnExistingDbWithIndexIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private DatabaseManagementService managementService;

    @Test
    public void startStopDatabaseWithIndex()
    {
        Label label = Label.label( "string" );
        String property = "property";
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService db = prepareDb( label, property, logProvider );
        managementService.shutdown();
        db = getDatabase( logProvider );
        managementService.shutdown();

        logProvider.rawMessageMatcher().assertNotContains( "Failed to open index" );
    }

    private GraphDatabaseService prepareDb( Label label, String propertyName, LogProvider logProvider )
    {
        GraphDatabaseService db = getDatabase( logProvider );
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyName ).create();
            transaction.commit();
        }
        waitIndexes( db );
        return db;
    }

    private GraphDatabaseService getDatabase( LogProvider logProvider )
    {
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setInternalLogProvider( logProvider )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void waitIndexes( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            transaction.commit();
        }
    }
}
