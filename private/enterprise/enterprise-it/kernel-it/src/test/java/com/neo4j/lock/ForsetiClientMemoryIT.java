/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.lock;

import com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiClient;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.ON_HEAP;
import static org.neo4j.io.ByteUnit.mebiBytes;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
public class ForsetiClientMemoryIT
{
    @Inject
    GraphDatabaseService databaseService;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.memory_transaction_max_size, mebiBytes( 1 ) )
                .setConfig( GraphDatabaseSettings.tx_state_memory_allocation, ON_HEAP );
    }

    @BeforeEach
    void setUp()
    {
        generateData();
    }

    @Test
    void detachedDeleteEntities()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            }
        } );
        verifyException( e );
    }

    @Test
    void deleteEntitiesUsingTransactionApiCalls()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.getAllNodes().stream().forEach( Node::delete );
            }
        } );
        verifyException( e );
    }

    @Test
    void exclusiveLockEntitiesUsingTransactionApiCalls()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.getAllNodes().stream().forEach( transaction::acquireWriteLock );
            }
        } );
        verifyException( e );
    }

    @Test
    void readLockEntitiesUsingTransactionApiCalls()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.getAllNodes().stream().forEach( transaction::acquireReadLock );
            }
        } );
        verifyException( e );
    }

    @Test
    void writeLockUnlockEntitiesUsingTransactionApiCalls()
    {
        assertDoesNotThrow( () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.getAllNodes().stream().forEach( node ->
                {
                    transaction.acquireWriteLock( node ).release();
                } );
            }
        } );
    }

    @Test
    void readLockUnlockEntitiesUsingTransactionApiCalls()
    {
        assertDoesNotThrow( () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                transaction.getAllNodes().stream().forEach( node ->
                {
                    transaction.acquireReadLock( node ).release();
                } );
            }
        } );
    }

    @Test
    void readWriteLockUnlockEntitiesUsingTransactionApiCalls()
    {
        assertDoesNotThrow( () ->
        {
            try ( var transaction = databaseService.beginTx() )
            {
                var memoryTracker = ((InternalTransaction) transaction).kernelTransaction().memoryTracker();
                long estimatedMemoryUsage = memoryTracker.estimatedHeapMemory();
                transaction.getAllNodes().stream().forEach( node ->
                {
                    long memoryUsageWithLock;
                    try ( var readLock = transaction.acquireReadLock( node );
                          var writeLock = transaction.acquireWriteLock( node ) )
                    {
                        // block that will take and release locks
                        memoryUsageWithLock = memoryTracker.estimatedHeapMemory();
                        assertThat( memoryUsageWithLock ).isGreaterThan( estimatedMemoryUsage );
                    }
                    assertThat( memoryTracker.estimatedHeapMemory() ).isLessThan( memoryUsageWithLock );
                } );
            }
        } );
    }

    private void generateData()
    {
        for ( int j = 0; j < 1000; j++ )
        {
            try ( var transaction = databaseService.beginTx() )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    transaction.createNode();
                }
                transaction.commit();
            }
        }
    }

    private static void verifyException( Exception e )
    {
        assertThat( e ).isExactlyInstanceOf( MemoryLimitExceededException.class )
                .hasMessageContaining( "The allocation of an extra 56 B would use more than the limit" )
                .hasStackTraceContaining( ForsetiClient.class.getSimpleName() );
    }
}
