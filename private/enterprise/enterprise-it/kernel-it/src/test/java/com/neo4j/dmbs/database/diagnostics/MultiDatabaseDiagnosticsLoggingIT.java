/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database.diagnostics;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class MultiDatabaseDiagnosticsLoggingIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private final AssertableLogProvider provider = new AssertableLogProvider();
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setInternalLogProvider( provider )
                .build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void dumpDefaultDatabaseInformation()
    {
        provider.rawMessageMatcher().assertContains( "Database: neo4j" );
        provider.rawMessageMatcher().assertContains( "Version" );
        provider.rawMessageMatcher().assertContains( "Store files" );
        provider.rawMessageMatcher().assertContains( "Transaction log" );
        provider.rawMessageMatcher().assertContains( "Id usage" );
        provider.rawMessageMatcher().assertContains( "Neostore records" );
        provider.rawMessageMatcher().assertContains( "Store versions" );
    }

    @Test
    void dumpDbInformationOnCreation() throws DatabaseExistsException
    {
        provider.clear();
        provider.assertNoLoggingOccurred();

        managementService.createDatabase( "NewDatabase" );
        provider.rawMessageMatcher().assertContains( "Database: newdatabase" );
        provider.rawMessageMatcher().assertContains( "Version" );
        provider.rawMessageMatcher().assertContains( "Store files" );
        provider.rawMessageMatcher().assertContains( "Transaction log" );
        provider.rawMessageMatcher().assertContains( "Id usage" );
        provider.rawMessageMatcher().assertContains( "Neostore records" );
        provider.rawMessageMatcher().assertContains( "Store versions" );
    }
}
