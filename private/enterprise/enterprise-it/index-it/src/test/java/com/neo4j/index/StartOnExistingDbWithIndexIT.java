/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.index;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class StartOnExistingDbWithIndexIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @Test
    void startStopDatabaseWithIndex()
    {
        Label label = Label.label( "string" );
        String property = "property";
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        prepareDb( label, property, logProvider );
        managementService.shutdown();
        getDatabase( logProvider );
        managementService.shutdown();

        assertThat( logProvider ).doesNotContainMessage( "Failed to open index" );
    }

    private void prepareDb( Label label, String propertyName, LogProvider logProvider )
    {
        GraphDatabaseService db = getDatabase( logProvider );
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.schema().constraintFor( label ).assertPropertyIsUnique( propertyName ).create();
            transaction.commit();
        }
        waitIndexes( db );
    }

    private GraphDatabaseService getDatabase( LogProvider logProvider )
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setInternalLogProvider( logProvider )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void waitIndexes( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            transaction.commit();
        }
    }
}
