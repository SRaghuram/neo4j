/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
class SystemDatabaseDatabaseManagementIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;
    private GraphDatabaseService systemDatabaseFacade;

    @BeforeEach
    void setUp()
    {
        managementService = createManagementService();
        systemDatabaseFacade = managementService.database( SYSTEM_DATABASE_NAME );
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
    void createDatabaseUsingCypherStatement()
    {
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( "foo" ) );
        executeInSystemDatabase( "CREATE DATABASE foo" );
        assertNotNull( managementService.database( "foo" ) );
    }

    @Test
    void keepActiveDatabaseStateBetweenRestarts()
    {
        executeInSystemDatabase( "CREATE DATABASE foo" );
        assertNotNull( managementService.database( "foo" ) );
        managementService.shutdown();
        managementService = createManagementService();
        assertNotNull( managementService.database( "foo" ) );
    }

    @Test
    void keepStoppedDatabaseStateBetweenRestarts()
    {
        executeInSystemDatabase( "CREATE DATABASE foo" );
        assertNotNull( managementService.database( "foo" ) );
        executeInSystemDatabase( "STOP DATABASE foo" );
        assertNotAvailable();
        managementService.shutdown();
        managementService = createManagementService();
        assertNotNull( managementService.database( "foo" ) );
        assertNotAvailable();
    }

    @Test
    void stopDatabaseUsingCypherStatement()
    {
        executeInSystemDatabase( "CREATE DATABASE foo" );
        executeInSystemDatabase( "STOP DATABASE foo" );
        assertNotAvailable();
    }

    @Test
    void startDatabaseUsingCypherStatement()
    {
        executeInSystemDatabase( "CREATE DATABASE foo" );
        executeInSystemDatabase( "START DATABASE foo" );
        assertAvailable();
    }

    @Test
    void restartDatabaseUsingCypherStatement()
    {
        executeInSystemDatabase( "CREATE DATABASE foo" );
        executeInSystemDatabase( "STOP DATABASE foo" );
        assertNotAvailable();

        executeInSystemDatabase( "START DATABASE foo" );
        assertAvailable();
    }

    @Test
    void dropDatabaseUsingCypherStatement()
    {
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( "foo" ) );
        executeInSystemDatabase( "CREATE DATABASE foo" );
        GraphDatabaseService oldFacade = managementService.database( "foo" );
        executeInSystemDatabase( "DROP DATABASE foo" );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( "foo" ) );
        assertThrows( DatabaseShutdownException.class, oldFacade::beginTx );
    }

    private void assertNotAvailable()
    {
        assertFalse( managementService.database( "foo" ).isAvailable( 0 ) );
    }

    private void assertAvailable()
    {
        assertTrue( managementService.database( "foo" ).isAvailable( 0 ) );
    }

    private void executeInSystemDatabase( String s )
    {
        try ( Transaction transaction = systemDatabaseFacade.beginTx() )
        {
            transaction.execute( s );
            transaction.commit();
        }
    }

    private DatabaseManagementService createManagementService()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
    }

}
