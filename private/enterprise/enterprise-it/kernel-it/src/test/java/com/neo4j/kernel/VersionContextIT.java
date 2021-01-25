/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.kernel.impl.pagecache.PageCacheWarmerExtensionFactory;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
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

@EnterpriseDbmsExtension( configurationCallback = "configure" )
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
        builder.setConfig( GraphDatabaseInternalSettings.snapshot_query, true )
               //  The global metrics extension and page cache warmer issue queries that can make our version contexts dirty.
               // If we don't remove these extensions, we might geb a count of 0 or more than 1 for `testCursorContext.getAdditionalAttempts()`,
               // depending on when the extension marks it as dirty
               .removeExtensions( extension -> extension instanceof GlobalMetricsExtensionFactory ||
                                                   extension instanceof PageCacheWarmerExtensionFactory );
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
            transaction.execute( "match (n) return n" );
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
                transaction.createNode();
                transaction.commit();
            }
        }
    }
}
