/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.count;

@ExtendWith( TestDirectoryExtension.class )
class CommercialSystemDatabaseIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseManager<?> databaseManager;
    private GraphDatabaseFacade defaultDb;
    private GraphDatabaseFacade systemDb;
    private DatabaseManagementService managementService;
    private DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @BeforeEach
    void setUp()
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( SystemGraphInitializer.NO_OP );   // disable system graph construction because it will interfere with some tests
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() ).setExternalDependencies( dependencies ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        databaseManager = getDatabaseManager( database );
        defaultDb = getDatabaseByName( databaseManager, databaseIdRepository.defaultDatabase() );
        systemDb = getDatabaseByName( databaseManager, databaseIdRepository.systemDatabase() );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
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
            assertEquals( 0, count( defaultDb.getAllLabels() ) );
        }
        try ( Transaction ignored = systemDb.beginTx() )
        {
            assertEquals( 1, count( systemDb.findNodes( systemLabel ) ) );
            assertEquals( 1, count( systemDb.getAllLabels() ) );
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
        assertNotEquals( defaultLogFiles.logFilesDirectory(), systemLogFiles.logFilesDirectory() );
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

        managementService.shutdown();

        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        assertTrue( runConsistencyCheck( systemDatabaseLayout, consistencyCheckService ).isSuccessful() );
        assertTrue( runConsistencyCheck( defaultDbLayout, consistencyCheckService ).isSuccessful() );
    }

    @Test
    void systemDatabaseEnabledByDefault()
    {
        GraphDatabaseService databaseWithSystemDb = null;
        DatabaseManagementService managementService = null;
        try
        {
            File disabledSystemDbDirectory = testDirectory.databaseDir( "withSystemDd" );
            managementService = new TestCommercialDatabaseManagementServiceBuilder( disabledSystemDbDirectory ).build();
            databaseWithSystemDb = managementService.database( DEFAULT_DATABASE_NAME );
            DatabaseManager<?> databaseManager = getDatabaseManager( databaseWithSystemDb );
            assertTrue( databaseManager.getDatabaseContext( databaseIdRepository.systemDatabase() ).isPresent() );
        }
        finally
        {
            if ( databaseWithSystemDb != null )
            {
                managementService.shutdown();
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

    private static GraphDatabaseFacade getDatabaseByName( DatabaseManager<?> databaseManager, DatabaseId databaseId )
    {
        return databaseManager.getDatabaseContext( databaseId ).orElseThrow( IllegalStateException::new ).databaseFacade();
    }

    private static DatabaseManager<?> getDatabaseManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
