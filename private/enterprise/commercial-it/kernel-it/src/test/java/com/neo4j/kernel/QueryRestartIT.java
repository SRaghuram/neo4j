/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.kernel.impl.pagecache.PageCacheWarmerExtensionFactory;
import com.neo4j.metrics.global.GlobalMetricsExtensionFactory;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.snapshot.TestVersionContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {TestDirectoryExtension.class} )
class QueryRestartIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;
    private TestTransactionVersionContextSupplier testContextSupplier;
    private TestVersionContext testCursorContext;

    @BeforeEach
    void setUp()
    {
        testContextSupplier = new TestTransactionVersionContextSupplier();
        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setExternalDependencies( dependencies )
                .setConfig( GraphDatabaseSettings.snapshot_query, true )
                //  The global metrics extension and page cache warmer issue queries that can make our version contexts dirty.
                // If we don't remove these extensions, we might geb a count of 0 or more than 1 for `testCursorContext.getAdditionalAttempts()`,
                // depending on when the extension marks it as dirty
                .removeExtensions( extension -> extension instanceof GlobalMetricsExtensionFactory ||
                                                    extension instanceof PageCacheWarmerExtensionFactory )
                .build();
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
    void executeQueryWithSingleRetryOnDefaultDatabase()
    {
        prepareCursorContext( DEFAULT_DATABASE_NAME );
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        createData( database );
        var result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        while ( result.hasNext() )
        {
            assertEquals( "d", result.next().get( "n.c" ) );
        }
    }

    @Test
    void executeQueryWithSingleRetryOnNonDefaultDatabase()
    {
        var databaseName = "futurama";
        managementService.createDatabase( databaseName );
        prepareCursorContext( databaseName );
        GraphDatabaseService database = managementService.database( databaseName );
        createData( database );
        var result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        while ( result.hasNext() )
        {
            assertEquals( "d", result.next().get( "n.c" ) );
        }
    }

    private void prepareCursorContext( String databaseName )
    {
        testCursorContext = TestVersionContext.testCursorContext( managementService, databaseName );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    private static void createData( GraphDatabaseService database )
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            transaction.commit();
        }
    }

}
