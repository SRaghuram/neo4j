/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseIdFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconcilerRequestTest
{

    @Test
    void shouldCorrectlyHandleRequestsWithMixedTargetPriority()
    {
        // given
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var baz = DatabaseIdFactory.from( "baz", UUID.randomUUID() );
        var mixed = ReconcilerRequest.targets( Set.of( foo, baz ) )
                                     .priorityTargets( Set.of( foo ) )
                                     .build();

        // when/then
        assertFalse( mixed.isSimple() );
        assertTrue( mixed.shouldBeExecutedAsPriorityFor( foo.name() ) );
        assertFalse( mixed.shouldBeExecutedAsPriorityFor( baz.name() ) );
        assertThat( mixed.explicitTargets() ).contains( foo.name(), baz.name() );
        assertThat( mixed.explicitTargets() ).doesNotContain( bar.name() );
    }

    @Test
    void shouldCorrectlyIdentifySimpleRequests()
    {
        // given
        var simple = ReconcilerRequest.simple();
        var priority = ReconcilerRequest.priorityTarget( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ).build();
        var explicit = ReconcilerRequest.targets( Set.of( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ) ).build();
        var panicked = ReconcilerRequest.panickedTarget( DatabaseIdFactory.from( "bar", UUID.randomUUID() ), new RuntimeException() ).build();

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
        var priorityA = ReconcilerRequest.priorityTargets( Set.of( foo, bar ) ).build();
        var priorityB = ReconcilerRequest.priorityTarget( bar ).build();

        // when/then
        assertTrue( priorityA.explicitTargets().contains( "foo" ) );
        assertTrue( priorityA.explicitTargets().contains( "bar" ) );
        assertFalse( priorityB.explicitTargets().contains( "foo" ) );
    }

    @Test
    void panicRequestDoNotUseCacheButHealAndUsePriority()
    {
        // given
        var panicked = ReconcilerRequest.panickedTarget( DatabaseIdFactory.from( "foo", UUID.randomUUID() ), new RuntimeException() ).build();

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
        var priority = ReconcilerRequest.priorityTarget( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ).build();

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
        var explicit = ReconcilerRequest.targets( Set.of( DatabaseIdFactory.from( "foo", UUID.randomUUID() ) ) ).build();

        // when/then
        assertFalse( explicit.shouldBeExecutedAsPriorityFor( "foo" ) );
        assertTrue( explicit.overridesPreviousFailuresFor( "foo" ) );
        assertFalse( explicit.canUseCacheFor( "foo" ) );

        assertFalse( explicit.shouldBeExecutedAsPriorityFor( "bar" ) );
        assertFalse( explicit.overridesPreviousFailuresFor( "bar" ) );
        assertTrue( explicit.canUseCacheFor( "bar" ) );
    }
}
