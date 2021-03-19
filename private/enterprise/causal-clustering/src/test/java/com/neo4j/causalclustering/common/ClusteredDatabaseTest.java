/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class ClusteredDatabaseTest
{

    private final Lifecycle componentA = mock( Lifecycle.class );
    private final Lifecycle componentB = mock( Lifecycle.class );
    private final Database kernel = mock( Database.class );
    private final Lifecycle componentC = mock( Lifecycle.class );
    private final Lifecycle componentD = mock( Lifecycle.class );

    @Test
    void shouldNotAllowRestart()
    {
        var kernel = mock( Database.class );
        var database = new ClusteredDatabase( List.of( kernel ) );

        database.start();
        assertTrue( database.hasBeenStarted() );

        database.stop();
        assertTrue( database.hasBeenStarted() );

        assertThrows( IllegalStateException.class, database::start );
    }

    @Test
    void shouldShutdownComponentsOnFailure() throws Exception
    {
        doThrow( RuntimeException.class ).when( componentC ).start();

        var database = new ClusteredDatabase( List.of( componentA, componentB, kernel, componentC, componentD ) );

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

    @Test
    void shouldCleanlyStartBuiltDatabase() throws Exception
    {
        var database = ClusteredDatabase.builder( ClusteredDatabase::new )
                                         .withComponent( componentA )
                                         .withKernelDatabase( kernel )
                                         .withComponent( componentB )
                                         .build();

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

}
