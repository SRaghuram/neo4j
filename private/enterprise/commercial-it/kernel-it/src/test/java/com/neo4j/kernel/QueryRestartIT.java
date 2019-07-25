/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.LongSupplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
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
        testCursorContext = testCursorContext( databaseName );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    private static void createData( GraphDatabaseService database )
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            transaction.success();
        }
    }

    private TestVersionContext testCursorContext( String databaseName )
    {
        TransactionIdStore transactionIdStore = getTransactionIdStore( databaseName );
        return new TestVersionContext( transactionIdStore::getLastClosedTransactionId );
    }

    private TransactionIdStore getTransactionIdStore( String databaseName )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) managementService.database( databaseName )).getDependencyResolver();
        return dependencyResolver.resolveDependency( TransactionIdStore.class );
    }

    private class TestVersionContext extends TransactionVersionContext
    {
        private boolean wrongLastClosedTxId = true;
        private int additionalAttempts;

        TestVersionContext( LongSupplier transactionIdSupplier )
        {
            super( transactionIdSupplier );
        }

        @Override
        public long lastClosedTransactionId()
        {
            return wrongLastClosedTxId ? TransactionIdStore.BASE_TX_ID : super.lastClosedTransactionId();
        }

        @Override
        public void markAsDirty()
        {
            super.markAsDirty();
            wrongLastClosedTxId = false;
        }

        @Override
        public boolean isDirty()
        {
            boolean dirty = super.isDirty();
            if ( dirty )
            {
                additionalAttempts++;
            }
            return dirty;
        }

        int getAdditionalAttempts()
        {
            return additionalAttempts;
        }
    }

    private static class TestTransactionVersionContextSupplier extends TransactionVersionContextSupplier
    {
        void setCursorContext( VersionContext versionContext )
        {
            this.cursorContext.set( versionContext );
        }
    }
}
