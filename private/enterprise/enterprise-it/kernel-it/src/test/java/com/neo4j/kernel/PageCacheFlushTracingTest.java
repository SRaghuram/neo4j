/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.LogAssertions.assertThat;

@TestDirectoryExtension
class PageCacheFlushTracingTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void tracePageCacheFlushProgress()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        DatabaseManagementService managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setInternalLogProvider( logProvider )
                .setConfig( GraphDatabaseInternalSettings.tracer, "verbose" )
                .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
        managementService.shutdown();
        assertThat( logProvider ).containsMessages( "Flushing file" );
    }
}
