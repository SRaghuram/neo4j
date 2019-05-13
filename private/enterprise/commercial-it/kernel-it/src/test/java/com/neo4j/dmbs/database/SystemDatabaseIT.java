/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
public class SystemDatabaseIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService databaseManagementService;
    private DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @BeforeEach
    void setUp()
    {
        databaseManagementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
    }

    @AfterEach
    void tearDown()
    {
        if ( databaseManagementService != null )
        {
            databaseManagementService.shutdown();
        }
    }

    @Test
    void checkIfDatabaseIsSystem()
    {
        GraphDatabaseService database = databaseManagementService.database( DEFAULT_DATABASE_NAME );
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        DatabaseManager<?> databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );

        assertFalse( getDatabaseByName( databaseManager, DEFAULT_DATABASE_NAME ).isSystem() );

        assertTrue( getDatabaseByName( databaseManager, SYSTEM_DATABASE_NAME ).isSystem() );
    }

    private Database getDatabaseByName( DatabaseManager<?> databaseManager, String name )
    {
        return databaseManager.getDatabaseContext( databaseIdRepository.get( name ) ).get().database();
    }
}
