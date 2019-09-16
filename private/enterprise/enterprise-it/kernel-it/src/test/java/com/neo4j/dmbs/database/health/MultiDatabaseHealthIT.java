/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database.health;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.CompositeDatabaseHealth;
import org.neo4j.monitoring.DatabaseHealth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiDatabaseHealthIT
{

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build();
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void databaseHealthIsTrackedAfterDatabaseRestart()
    {
        String testDatabaseName = "testDatabase";
        managementService.createDatabase( testDatabaseName );
        CompositeDatabaseHealth globalHealth = getGlobalHealth( testDatabaseName );

        checkDatabaseAndGlobalLife( globalHealth, getDatabaseHealth( testDatabaseName ) );

        managementService.shutdownDatabase( testDatabaseName );
        managementService.startDatabase( testDatabaseName );

        checkDatabaseAndGlobalLife( globalHealth, getDatabaseHealth( testDatabaseName ) );
    }

    private static void checkDatabaseAndGlobalLife( CompositeDatabaseHealth globalHealth, DatabaseHealth databaseHealth )
    {
        assertTrue( databaseHealth.isHealthy() );
        assertTrue( globalHealth.isHealthy() );

        databaseHealth.panic( new RuntimeException( "any" ) );

        assertFalse( databaseHealth.isHealthy() );
        assertFalse( globalHealth.isHealthy() );

        assertTrue( databaseHealth.healed() );

        assertTrue( databaseHealth.isHealthy() );
        assertTrue( globalHealth.isHealthy() );
    }

    private DatabaseHealth getDatabaseHealth( String testDatabaseName )
    {
        return getDependencyResolver( testDatabaseName ).resolveDependency( DatabaseHealth.class );
    }

    private CompositeDatabaseHealth getGlobalHealth( String testDatabaseName )
    {
        return getDependencyResolver( testDatabaseName ).resolveDependency( CompositeDatabaseHealth.class );
    }

    private DependencyResolver getDependencyResolver( String testDatabaseName )
    {
        return ((GraphDatabaseAPI) managementService.database( testDatabaseName )).getDependencyResolver();
    }
}
