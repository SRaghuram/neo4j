/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@CommercialDbmsExtension( configurationCallback = "configure" )
class VersionContextIT
{
    @Inject
    private DatabaseManagementService managementService;
    private static final String databaseName = "futurama";
    private GraphDatabaseAPI futuramaDatabase;
    private GraphDatabaseAPI defaultDatabase;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.snapshot_query, true );
    }

    @BeforeAll
    void setUp()
    {
        managementService.createDatabase( databaseName );
        futuramaDatabase = (GraphDatabaseAPI) managementService.database( databaseName );
        defaultDatabase = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @Test
    void differentDatabaseHaveDifferentVersionContext()
    {
        VersionContextSupplier futuramaContextSupplier = getVersionContext( futuramaDatabase );
        VersionContextSupplier defaultContextSupplier = getVersionContext( defaultDatabase );

        assertNotSame( futuramaContextSupplier, defaultContextSupplier );
    }

    @Test
    void ongoingTransactionDoesNotInfluenceContextInDifferentDatabase()
    {
        VersionContextSupplier futuramaContextSupplier = getVersionContext( futuramaDatabase );
        VersionContextSupplier defaultContextSupplier = getVersionContext( defaultDatabase );
        createData( futuramaDatabase );
        createData( defaultDatabase );

        try ( Transaction transaction = futuramaDatabase.beginTx() )
        {
            futuramaDatabase.execute( "match (n) return n" );
            assertNotSame( futuramaContextSupplier.getVersionContext().lastClosedTransactionId(),
                    defaultContextSupplier.getVersionContext().lastClosedTransactionId() );
        }
    }

    private static VersionContextSupplier getVersionContext( GraphDatabaseAPI databaseAPI )
    {
        return databaseAPI.getDependencyResolver().resolveDependency( VersionContextSupplier.class );
    }

    private static void createData( GraphDatabaseService database )
    {
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.createNode();
                transaction.commit();
            }
        }
    }
}
