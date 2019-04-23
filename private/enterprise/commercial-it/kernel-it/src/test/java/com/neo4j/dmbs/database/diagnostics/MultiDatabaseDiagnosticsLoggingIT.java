/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database.diagnostics;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseDiagnosticsLoggingIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider provider = new AssertableLogProvider();
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
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
        provider.assertContainsMessageContaining( "Database: neo4j" );
        provider.assertContainsMessageContaining( "Version" );
        provider.assertContainsMessageContaining( "Store files" );
        provider.assertContainsMessageContaining( "Transaction log" );
        provider.assertContainsMessageContaining( "Id usage" );
        provider.assertContainsMessageContaining( "Neostore records" );
        provider.assertContainsMessageContaining( "Store versions" );
    }

    @Test
    void dumpDbInformationOnCreation() throws DatabaseExistsException
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        provider.clear();
        provider.assertNoLoggingOccurred();

        DatabaseManager<?> databaseManager = resolver.resolveDependency( DatabaseManager.class );
        databaseManager.createDatabase( new DatabaseId( "NewDatabase" ) );
        provider.assertContainsMessageContaining( "Database: NewDatabase" );
        provider.assertContainsMessageContaining( "Version" );
        provider.assertContainsMessageContaining( "Store files" );
        provider.assertContainsMessageContaining( "Transaction log" );
        provider.assertContainsMessageContaining( "Id usage" );
        provider.assertContainsMessageContaining( "Neostore records" );
        provider.assertContainsMessageContaining( "Store versions" );
    }
}
