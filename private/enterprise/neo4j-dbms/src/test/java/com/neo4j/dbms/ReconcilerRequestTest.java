/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconcilerRequestTest
{
    @Test
    void shouldCorrectlyIdentifySimpleRequests()
    {
        // given
        var simple = ReconcilerRequest.simple();
        var priority = ReconcilerRequest.priority( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) );
        var explicit = ReconcilerRequest.explicit( Set.of( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ) );
        var panicked = ReconcilerRequest.forPanickedDatabase( DatabaseIdFactory.from( "bar", UUID.randomUUID() ), new RuntimeException() );

        // when/then
        assertTrue( simple.isSimple() );
        assertFalse( priority.isSimple() );
        assertFalse( explicit.isSimple() );
        assertFalse( panicked.isSimple() );
    }

    @Test
    void shouldCorrectlyIdentifyPriorityRequests()
    {
        // given
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var priorityA = ReconcilerRequest.priority( Set.of( foo, bar ) );
        var priorityB = ReconcilerRequest.priority( bar );

        // when/then
        assertTrue( priorityA.specifiedDatabaseNames().contains( "foo" ) );
        assertTrue( priorityA.specifiedDatabaseNames().contains( "bar" ) );
        assertFalse( priorityB.specifiedDatabaseNames().contains(  "foo" ) );
    }

    @Test
    void panicRequestDoNotUseCacheButHealAndUsePriority()
    {
        // given
        var panicked = ReconcilerRequest.forPanickedDatabase( DatabaseIdFactory.from( "foo", UUID.randomUUID() ), new RuntimeException() );

        // when/then
        assertTrue( panicked.shouldBeExecutedAsPriorityFor( "foo" ) );
        assertTrue( panicked.overridesPreviousFailuresFor( "foo" ) );
        assertFalse( panicked.canUseCacheFor( "foo" ) );

        assertFalse( panicked.shouldBeExecutedAsPriorityFor( "bar" ) );
        assertFalse( panicked.overridesPreviousFailuresFor( "bar" ) );
        assertTrue( panicked.canUseCacheFor( "bar" ) );
    }

    @Test
    void priorityRequestDoNotUseCacheButHealAndUsePriority()
    {
        // given
        var priority = ReconcilerRequest.priority( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) );

        // when/then
        assertTrue( priority.shouldBeExecutedAsPriorityFor( "foo" ) );
        assertTrue( priority.overridesPreviousFailuresFor( "foo" ) );
        assertFalse( priority.canUseCacheFor( "foo" ) );

        assertFalse( priority.shouldBeExecutedAsPriorityFor( "bar" ) );
        assertFalse( priority.overridesPreviousFailuresFor( "bar" ) );
        assertTrue( priority.canUseCacheFor( "bar" ) );
    }

    @Test
    void explicitRequestDoNotUseCacheAndPriorityButHeal()
    {
        // given
        var explicit = ReconcilerRequest.explicit( Set.of( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ) );

        // when/then
        assertFalse( explicit.shouldBeExecutedAsPriorityFor( "foo" ) );
        assertTrue( explicit.overridesPreviousFailuresFor( "foo" ) );
        assertFalse( explicit.canUseCacheFor( "foo" ) );

        assertFalse( explicit.shouldBeExecutedAsPriorityFor( "bar" ) );
        assertFalse( explicit.overridesPreviousFailuresFor( "bar" ) );
        assertTrue( explicit.canUseCacheFor( "bar" ) );
    }
}
