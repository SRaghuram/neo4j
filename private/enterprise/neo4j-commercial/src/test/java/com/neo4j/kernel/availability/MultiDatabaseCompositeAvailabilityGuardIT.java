/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.availability;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseCompositeAvailabilityGuardIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void globalCompositeGuardUsedInTransactionBridge()
    {
        ThreadToStatementContextBridge bridge = getTransactionBridge();
        DatabaseManager databaseManager = getDatabaseManager();
        GraphDatabaseFacade secondDatabase = databaseManager.createDatabase( "second.db" );
        secondDatabase.shutdown();

        assertThrows( DatabaseShutdownException.class, bridge::assertInUnterminatedTransaction );
    }

    private ThreadToStatementContextBridge getTransactionBridge()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
