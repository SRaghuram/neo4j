/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class DefaultDatabaseDropIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void dropDefaultDatabaseAndRestart()
    {
        managementService = createManagementService();
        assertTrue( managementService.database( DEFAULT_DATABASE_NAME ).isAvailable( 0 ) );

        managementService.dropDatabase( DEFAULT_DATABASE_NAME );

        managementService.shutdown();
        managementService = createManagementService();
    }

    @Test
    void shutdownDefaultDatabaseAndRestart()
    {
        managementService = createManagementService();
        assertTrue( managementService.database( DEFAULT_DATABASE_NAME ).isAvailable( 0 ) );

        managementService.shutdownDatabase( DEFAULT_DATABASE_NAME );

        managementService.shutdown();
        managementService = createManagementService();
    }

    private DatabaseManagementService createManagementService()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
    }
}
