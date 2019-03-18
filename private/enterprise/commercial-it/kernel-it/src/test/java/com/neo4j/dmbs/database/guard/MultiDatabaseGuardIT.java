/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database.guard;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseGuardIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
        }
    }

    @Test
    void databaseGuardDynamicRegistration()
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        DatabaseManager<?> databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
        CompositeDatabaseAvailabilityGuard compositeGuard =
                dependencyResolver.resolveDependency( CompositeDatabaseAvailabilityGuard.class );

        assertEquals( 2, compositeGuard.getGuards().size() );

        String firstDatabase = "Fry";
        String secondDatabase = "Lila";
        String thirdDatabase = "Bender";

        databaseManager.createDatabase( firstDatabase );
        assertEquals( 3, compositeGuard.getGuards().size() );

        databaseManager.createDatabase( secondDatabase );
        databaseManager.createDatabase( thirdDatabase );
        assertEquals( 5, compositeGuard.getGuards().size() );

        databaseManager.stopDatabase( thirdDatabase );
        assertEquals( 4, compositeGuard.getGuards().size() );

        databaseManager.stopDatabase( firstDatabase );
        databaseManager.stopDatabase( secondDatabase );

        assertEquals( 2, compositeGuard.getGuards().size() );
    }
}
