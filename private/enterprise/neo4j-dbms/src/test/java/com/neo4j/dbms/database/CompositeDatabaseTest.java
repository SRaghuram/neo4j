/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class CompositeDatabaseTest
{
    @Test
    void shouldCleanlyStartWhenNoFailures() throws Throwable
    {
        var componentA = mock( Lifecycle.class );
        var kernel = mock( Database.class );
        var componentB = mock( Lifecycle.class );

        var database = new CompositeDatabase( List.of( componentA ), kernel, List.of( componentB ) );

        database.start();

        InOrder inOrder = inOrder( componentA, kernel, componentB );

        inOrder.verify( componentA ).init();
        inOrder.verify( kernel ).init();
        inOrder.verify( componentB ).init();

        inOrder.verify( componentA ).start();
        inOrder.verify( kernel ).start();
        inOrder.verify( componentB ).start();

        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldCleanupComponentsOnFailure() throws Exception
    {
        var componentA = mock( Lifecycle.class );
        var componentB = mock( Lifecycle.class );
        var kernel = mock( Database.class );
        var componentC = mock( Lifecycle.class );
        var componentD = mock( Lifecycle.class );

        doThrow( RuntimeException.class ).when( componentC ).start();

        var database = new CompositeDatabase( List.of( componentA, componentB ), kernel, List.of( componentC, componentD ) );

        assertThrows( LifecycleException.class, database::start );

        InOrder inOrder = inOrder( componentA, componentB, kernel, componentC, componentD );

        inOrder.verify( componentA ).init();
        inOrder.verify( componentB ).init();
        inOrder.verify( kernel ).init();
        inOrder.verify( componentC ).init();
        inOrder.verify( componentD ).init();

        inOrder.verify( componentA ).start();
        inOrder.verify( componentB ).start();
        inOrder.verify( kernel ).start();
        inOrder.verify( componentC ).start();

        inOrder.verify( componentC ).stop();
        inOrder.verify( kernel ).stop();
        inOrder.verify( componentB ).stop();
        inOrder.verify( componentA ).stop();

        inOrder.verify( componentC ).shutdown();
        inOrder.verify( kernel ).shutdown();
        inOrder.verify( componentB ).shutdown();
        inOrder.verify( componentA ).shutdown();

        inOrder.verifyNoMoreInteractions();
    }
}
