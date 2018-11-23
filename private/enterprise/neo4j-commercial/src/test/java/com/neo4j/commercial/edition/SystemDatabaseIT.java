/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.auth_provider;

@ExtendWith( TestDirectoryExtension.class )
class SystemDatabaseIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseManager databaseManager;
    private GraphDatabaseFacade defaultDb;
    private GraphDatabaseFacade systemDb;

    @BeforeEach
    void setUp()
    {
        database = new CommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.databaseDir() )
                .setConfig( auth_provider, SYSTEM_GRAPH_REALM_NAME ).newGraphDatabase();
        databaseManager = getDatabaseManager( database );
        defaultDb = getDatabaseByName( databaseManager, DEFAULT_DATABASE_NAME );
        systemDb = getDatabaseByName( databaseManager, SYSTEM_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void systemAndDefaultDatabasesAvailable()
    {
        assertNotNull( defaultDb );
        assertNotNull( systemDb );
        assertNotSame( defaultDb, systemDb );
    }

    @Test
    void systemDatabaseDataNotAvailableInDefaultDatabase()
    {
        Label systemLabel = label( "systemLabel" );
        try ( Transaction transaction = systemDb.beginTx() )
        {
            Node node = systemDb.createNode( systemLabel );
            node.setProperty( "a", "b" );
            transaction.success();
        }
        try ( Transaction ignored = defaultDb.beginTx() )
        {
            assertEquals( 0, count( defaultDb.findNodes( systemLabel ) ) );
        }
    }

    @Test
    void separateTransactionLogsForSystemDatabase() throws IOException
    {
        int systemDatabaseTransactions = 100;
        int defaultDatabaseTransactions = 15;

        for ( int i = 0; i < systemDatabaseTransactions; i++ )
        {
            try ( Transaction transaction = systemDb.beginTx() )
            {
                Node nodeA = systemDb.createNode();
                Node nodeB = systemDb.createNode();
                nodeA.createRelationshipTo( nodeB, RelationshipType.withName( valueOf( i ) ) );
                transaction.success();
            }
        }

        for ( int i = 0; i < defaultDatabaseTransactions; i++ )
        {
            try ( Transaction transaction = defaultDb.beginTx() )
            {
                defaultDb.createNode( label( valueOf( i ) ) );
                transaction.success();
            }
        }

        countTransactionInLogicalStore( systemDb, systemDatabaseTransactions * 2 );
        countTransactionInLogicalStore( defaultDb, defaultDatabaseTransactions * 2);
    }

    @Test
    void differentDatabaseHaveDifferentTxLogsDirectories()
    {
        LogFiles systemLogFiles = systemDb.getDependencyResolver().resolveDependency( LogFiles.class );
        LogFiles defaultLogFiles = defaultDb.getDependencyResolver().resolveDependency( LogFiles.class );
        assertEquals( systemDb.databaseLayout().databaseDirectory(), systemLogFiles.logFilesDirectory() );
        assertEquals( defaultDb.databaseLayout().databaseDirectory(), defaultLogFiles.logFilesDirectory() );
    }

    @Test
    void systemAndDefaultDatabasesAreConsistentAfterShutdown() throws ConsistencyCheckIncompleteException
    {
        int systemDatabaseTransactions = 100;
        int defaultDatabaseTransactions = 15;
        DatabaseLayout systemDatabaseLayout = systemDb.databaseLayout();
        DatabaseLayout defaultDbLayout = defaultDb.databaseLayout();

        for ( int i = 0; i < systemDatabaseTransactions; i++ )
        {
            try ( Transaction transaction = systemDb.beginTx() )
            {
                Node nodeA = systemDb.createNode();
                Node nodeB = systemDb.createNode();
                nodeA.createRelationshipTo( nodeB, RelationshipType.withName( valueOf( i ) ) );
                transaction.success();
            }
        }

        for ( int i = 0; i < defaultDatabaseTransactions; i++ )
        {
            try ( Transaction transaction = defaultDb.beginTx() )
            {
                defaultDb.createNode( label( valueOf( i ) ) );
                transaction.success();
            }
        }

        database.shutdown();

        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        assertTrue( runConsistencyCheck( systemDatabaseLayout, consistencyCheckService ).isSuccessful() );
        assertTrue( runConsistencyCheck( defaultDbLayout, consistencyCheckService ).isSuccessful() );
    }

    @Test
    void systemDatabaseDisabledByDefault()
    {
        GraphDatabaseService databaseWithoutSystemDb = null;

        try
        {
            File disabledSystemDbDirectory = testDirectory.databaseDir( "disabledSystemDb" );
            databaseWithoutSystemDb = new CommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( disabledSystemDbDirectory ).newGraphDatabase();
            DatabaseManager databaseManager = getDatabaseManager( databaseWithoutSystemDb );
            assertFalse( databaseManager.getDatabaseContext( SYSTEM_DATABASE_NAME ).isPresent() );
        }
        finally
        {
            if ( databaseWithoutSystemDb != null )
            {
                databaseWithoutSystemDb.shutdown();
            }
        }
    }

    private static ConsistencyCheckService.Result runConsistencyCheck( DatabaseLayout systemDatabaseLayout, ConsistencyCheckService consistencyCheckService )
            throws ConsistencyCheckIncompleteException
    {
        return consistencyCheckService.runFullConsistencyCheck( systemDatabaseLayout, Config.defaults(), ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), false );
    }

    private static void countTransactionInLogicalStore( GraphDatabaseFacade facade, int expectedTransactions ) throws IOException
    {
        LogicalTransactionStore transactionStore = facade.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        try ( TransactionCursor transactions = transactionStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            int counter = 0;
            while ( transactions.next() )
            {
                counter++;
            }
            assertEquals( expectedTransactions, counter );
        }
    }

    private static GraphDatabaseFacade getDatabaseByName( DatabaseManager databaseManager, String dbName )
    {
        return databaseManager.getDatabaseContext( dbName ).orElseThrow( IllegalStateException::new ).getDatabaseFacade();
    }

    private DatabaseManager getDatabaseManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
