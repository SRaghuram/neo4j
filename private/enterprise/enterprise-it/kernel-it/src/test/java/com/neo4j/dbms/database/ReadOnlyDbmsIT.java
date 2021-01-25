/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;

@TestDirectoryExtension
class ReadOnlyDbmsIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startDbmsReadOnlyMode()
    {
        var managementService = createDbms( false );
        try
        {
            var defaultDatabase = managementService.database( DEFAULT_DATABASE_NAME );
            var dbConfig = getDbConfig( defaultDatabase );
            assertFalse( dbConfig.get( read_only ) );
            assertDoesNotThrow( () -> createNodeTransaction( defaultDatabase ) );
        }
        finally
        {
            managementService.shutdown();
        }

        var readOnlyService = createDbms( true );
        try
        {
            var readOnlyDatabase = readOnlyService.database( DEFAULT_DATABASE_NAME );
            var dbConfig = getDbConfig( readOnlyDatabase );
            assertTrue( dbConfig.get( read_only ) );
            assertThrows( WriteOperationsNotAllowedException.class, () -> createNodeTransaction( readOnlyDatabase ) );
        }
        finally
        {
            readOnlyService.shutdown();
        }
    }

    private void createNodeTransaction( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode();
            transaction.commit();
        }
    }

    private Config getDbConfig( GraphDatabaseService defaultDatabase )
    {
        return ((GraphDatabaseAPI) defaultDatabase).getDependencyResolver().resolveDependency( Config.class );
    }

    private DatabaseManagementService createDbms( boolean readOnly )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( read_only, readOnly ).build();
    }
}
