/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database.diagnostics;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseDiagnosticsLoggingIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private AssertableLogProvider provider = new AssertableLogProvider();

    @BeforeEach
    void setUp()
    {
        database = new TestCommercialGraphDatabaseFactory().setInternalLogProvider( provider ).newEmbeddedDatabase( testDirectory.databaseDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void dumpDefaultDatabaseInformation()
    {
        provider.assertContainsMessageContaining( "Database: graph.db" );
        provider.assertContainsMessageContaining( "Version" );
        provider.assertContainsMessageContaining( "Store files" );
        provider.assertContainsMessageContaining( "Transaction log" );
        provider.assertContainsMessageContaining( "Id usage" );
        provider.assertContainsMessageContaining( "Neostore records" );
        provider.assertContainsMessageContaining( "Store versions" );
    }

    @Test
    void dumpDbInformationOnCreation()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        provider.clear();
        provider.assertNoLoggingOccurred();

        DatabaseManager databaseManager = resolver.resolveDependency( DatabaseManager.class );
        GraphDatabaseFacade newDatabase = databaseManager.createDatabase( "NewDatabase" );
        provider.assertContainsMessageContaining( "Database: NewDatabase" );
        provider.assertContainsMessageContaining( "Version" );
        provider.assertContainsMessageContaining( "Store files" );
        provider.assertContainsMessageContaining( "Transaction log" );
        provider.assertContainsMessageContaining( "Id usage" );
        provider.assertContainsMessageContaining( "Neostore records" );
        provider.assertContainsMessageContaining( "Store versions" );
    }
}
