/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class ClusteredDatabaseTest
{
    @Test
    void shouldCleanlyStartWhenNoFailures() throws Throwable
    {
        Lifecycle componentA = mock( Lifecycle.class );
        Lifecycle componentB = mock( Lifecycle.class );

        ClusteredDatabase database = new ClusteredDatabase();

        database.addComponent( componentA );
        database.addComponent( componentB );

        database.start();

        InOrder inOrder = inOrder( componentA, componentB );

        inOrder.verify( componentA ).init();
        inOrder.verify( componentB ).init();

        inOrder.verify( componentA ).start();
        inOrder.verify( componentB ).start();

        inOrder.verifyNoMoreInteractions();

        assertTrue( database.hasBeenStarted() );
    }

    @Test
    void shouldCleanupComponentsOnFailure() throws Exception
    {
        Lifecycle componentA = mock( Lifecycle.class );
        Lifecycle componentB = mock( Lifecycle.class );
        Lifecycle componentC = mock( Lifecycle.class );
        Lifecycle componentD = mock( Lifecycle.class );

        doThrow( RuntimeException.class ).when( componentC ).start();

        ClusteredDatabase database = new ClusteredDatabase();

        database.addComponent( componentA );
        database.addComponent( componentB );
        database.addComponent( componentC );
        database.addComponent( componentD );

        assertThrows( LifecycleException.class, database::start );

        InOrder inOrder = inOrder( componentA, componentB, componentC, componentD );

        inOrder.verify( componentA ).init();
        inOrder.verify( componentB ).init();
        inOrder.verify( componentC ).init();
        inOrder.verify( componentD ).init();

        inOrder.verify( componentA ).start();
        inOrder.verify( componentB ).start();
        inOrder.verify( componentC ).start();

        inOrder.verify( componentC ).stop();
        inOrder.verify( componentB ).stop();
        inOrder.verify( componentA ).stop();

        inOrder.verify( componentC ).shutdown();
        inOrder.verify( componentB ).shutdown();
        inOrder.verify( componentA ).shutdown();

        inOrder.verifyNoMoreInteractions();

        assertTrue( database.hasBeenStarted() );
    }

    @Test
    void shouldNotAllowRestart()
    {
        Lifecycle componentA = mock( Lifecycle.class );
        ClusteredDatabase database = new ClusteredDatabase();

        database.addComponent( componentA );

        database.start();
        assertTrue( database.hasBeenStarted() );

        database.stop();
        assertTrue( database.hasBeenStarted() );

        assertThrows( IllegalStateException.class, database::start );
    }
}
