/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class ExistingDatabaseCreationIT
{
    private static final String anotherDatabaseName = "anotherdatabase";
    private static final String cloneDatabase = "clonedatabase";
    private static final int NUMBER_OF_CREATED_NODES = 100;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

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
    void createDatabaseWithAlreadyExistingDatabase() throws IOException
    {
        managementService = startManagementService();

        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        createSomeNodes( database );

        managementService.createDatabase( anotherDatabaseName );

        GraphDatabaseAPI anotherDatabaseService = (GraphDatabaseAPI) managementService.database( anotherDatabaseName );
        createSomeNodes( anotherDatabaseService );
        DatabaseLayout databaseLayout = anotherDatabaseService.databaseLayout();
        managementService.shutdown();

        DatabaseLayout cloneLayout = testDirectory.databaseLayout( cloneDatabase );
        copyDatabase( databaseLayout, cloneLayout );

        managementService = startManagementService();
        managementService.createDatabase( cloneDatabase );
        GraphDatabaseService cloneDatabaseService = managementService.database( cloneDatabase );
        verifyExpectedNodeCounts( cloneDatabaseService );
    }

    private void verifyExpectedNodeCounts( GraphDatabaseService cloneDatabaseService )
    {
        try ( Transaction ignore = cloneDatabaseService.beginTx() )
        {
            assertEquals( NUMBER_OF_CREATED_NODES, Iterables.count( cloneDatabaseService.getAllNodes() ) );
        }
    }

    private DatabaseManagementService startManagementService()
    {
        return new CommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
    }

    private void copyDatabase( DatabaseLayout databaseLayout, DatabaseLayout cloneLayout ) throws IOException
    {
        fileSystem.copyRecursively( databaseLayout.databaseDirectory(), cloneLayout.databaseDirectory() );
        fileSystem.copyRecursively( databaseLayout.getTransactionLogsDirectory(), cloneLayout.getTransactionLogsDirectory() );
    }

    private void createSomeNodes( GraphDatabaseService database )
    {
        for ( int i = 0; i < NUMBER_OF_CREATED_NODES; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode();
                transaction.success();
            }
        }
    }
}
