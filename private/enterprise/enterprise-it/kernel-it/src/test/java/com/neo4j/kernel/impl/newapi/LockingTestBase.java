/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestSupport;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class LockingTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Test
    void shouldNotBlockConstraintCreationOnUnrelatedPropertyWrite() throws Throwable
    {
        int nodeProp;
        int constraintProp;
        int label;

        // Given
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeProp = tx.tokenWrite().propertyKeyGetOrCreateForName( "nodeProp" );
            constraintProp = tx.tokenWrite().propertyKeyGetOrCreateForName( "constraintProp" );
            label = tx.tokenWrite().labelGetOrCreateForName( "label" );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            IndexPrototype prototype = IndexPrototype.uniqueForSchema( labelDescriptor( label, constraintProp ) ).withName( "constraint name" );
            tx.schemaWrite().uniquePropertyConstraintCreate( prototype );
            tx.commit();
        }

        CountDownLatch createNodeLatch = new CountDownLatch( 1 );
        CountDownLatch createConstraintLatch = new CountDownLatch( 1 );

        // When & Then
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        Future<?> f1 = executor.submit( () -> {
            try ( KernelTransaction tx = beginTransaction() )
            {
                createNodeWithProperty( tx, nodeProp );

                createNodeLatch.countDown();
                assertTrue( createConstraintLatch.await( 5, TimeUnit.MINUTES) );

                tx.commit();
            }
            catch ( Exception e )
            {
                fail( "Create node failed: " + e );
            }
            finally
            {
                createNodeLatch.countDown();
            }
        } );

        Future<?> f2 = executor.submit( () -> {

            try ( KernelTransaction tx = beginTransaction() )
            {
                assertTrue( createNodeLatch.await( 5, TimeUnit.MINUTES) );
                IndexPrototype prototype = IndexPrototype.uniqueForSchema( labelDescriptor( label, constraintProp ) ).withName( "other constraint name" );
                tx.schemaWrite().uniquePropertyConstraintCreate( prototype );
                tx.commit();
            }
            catch ( KernelException e )
            {
                // constraint already exists, so should fail!
                assertEquals( Status.Schema.ConstraintAlreadyExists, e.status() );
            }
            catch ( InterruptedException e )
            {
                fail( "Interrupted during create constraint" );
            }
            finally
            {
                createConstraintLatch.countDown();
            }
        } );

        try
        {
            f1.get();
            f2.get();
        }
        finally
        {
            executor.shutdown();
        }
    }

    private void createNodeWithProperty( KernelTransaction tx, int propId1 ) throws KernelException
    {
        long node = tx.dataWrite().nodeCreate();
        tx.dataWrite().nodeSetProperty( node, propId1, Values.intValue( 42 ) );
    }

    protected abstract LabelSchemaDescriptor labelDescriptor( int label, int... props );
}
