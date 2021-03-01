/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.kernel.impl.locking.forseti.ForsetiLockManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
public class ForsetiLockAcquisitionTimeoutIT
{
    private final OtherThreadExecutor secondTransactionExecutor = new OtherThreadExecutor( "transactionExecutor" );
    private final OtherThreadExecutor clockExecutor = new OtherThreadExecutor( "clockExecutor" );

    private static final String TEST_PROPERTY_NAME = "a";
    private static final Label marker = Label.label( "marker" );
    private static final FakeClock fakeClock = Clocks.fakeClock();

    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = getDbmsb( testDirectory )
                .setClock( fakeClock )
                .setConfig( GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofSeconds( 2 ) )
                .build();
        database = managementService.database( DEFAULT_DATABASE_NAME );

        createTestNode( marker );
    }

    protected TestDatabaseManagementServiceBuilder getDbmsb( TestDirectory directory )
    {
        return new TestDatabaseManagementServiceBuilder( directory.homePath() );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
        secondTransactionExecutor.close();
        clockExecutor.close();
    }

    @Test
    void timeoutOnAcquiringExclusiveLock()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                ResourceIterator<Node> nodes = tx.findNodes( marker );
                Node node = nodes.next();
                node.setProperty( TEST_PROPERTY_NAME, "b" );

                Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( () ->
                {
                    try ( Transaction transaction1 = database.beginTx() )
                    {
                        transaction1.getNodeById( node.getId() ).setProperty( TEST_PROPERTY_NAME, "b" );
                        transaction1.commit();
                    }
                    return null;
                } );

                secondTransactionExecutor.waitUntilWaiting( exclusiveLockWaitingPredicate() );
                clockExecutor.execute( () ->
                {
                    fakeClock.forward( 3, TimeUnit.SECONDS );
                    return null;
                } );
                propertySetFuture.get();
            }
        } );
        assertThat( e ).hasRootCauseInstanceOf( LockAcquisitionTimeoutException.class ).hasMessageContaining(
                "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. " +
                        "Unable to acquire lock within configured timeout (dbms.lock.acquisition.timeout). " +
                        "Unable to acquire lock for resource: NODE with id: 0 within 2000 millis." );
    }

    @Test
    void timeoutOnAcquiringSharedLock()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                Locks lockManger = getLockManager();
                Locks.Client client = lockManger.newClient();
                client.initialize( LeaseService.NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE );
                client.acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 1 );

                Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( () ->
                {
                    try ( Transaction nestedTransaction = database.beginTx() )
                    {
                        ResourceIterator<Node> nodes = nestedTransaction.findNodes( marker );
                        Node node = nodes.next();
                        node.addLabel( Label.label( "anotherLabel" ) );
                        nestedTransaction.commit();
                    }
                    return null;
                } );

                secondTransactionExecutor.waitUntilWaiting( sharedLockWaitingPredicate() );
                clockExecutor.execute( () ->
                {
                    fakeClock.forward( 3, TimeUnit.SECONDS );
                    return null;
                } );
                propertySetFuture.get();
            }
        } );
        assertThat( e ).hasRootCauseInstanceOf( LockAcquisitionTimeoutException.class ).hasMessageContaining(
                "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. " +
                        "Unable to acquire lock within configured timeout (dbms.lock.acquisition.timeout). " +
                        "Unable to acquire lock for resource: LABEL with id: 1 within 2000 millis." );
    }

    protected DependencyResolver getDependencyResolver()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver();
    }
    private void createTestNode( Label marker )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode( marker );
            transaction.commit();
        }
    }

    protected static Predicate<OtherThreadExecutor.WaitDetails> exclusiveLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( ForsetiClient.class, "acquireExclusive" );
    }

    protected static Predicate<OtherThreadExecutor.WaitDetails> sharedLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( ForsetiClient.class, "acquireShared" );
    }

    protected Locks getLockManager()
    {
        return getDependencyResolver().resolveDependency( ForsetiLockManager.class );
    }
}
