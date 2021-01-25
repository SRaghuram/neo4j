/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpiringSetTest
{
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final Duration expiryPeriod = Duration.ofSeconds( 1 );
    private final ExpiringSet<Object> expiringSet = new ExpiringSet<>( expiryPeriod, fakeClock );
    private Object ob1 = new Object();
    private Object ob2 = new Object();

    @Test
    void shouldContainObject()
    {
        // when
        expiringSet.add( ob1 );

        // then
        hasObject( expiringSet, ob1 );
        assertTrue( expiringSet.nonEmpty() );
        assertFalse( expiringSet.isEmpty() );
    }

    @Test
    void shouldExpireObject()
    {
        // when
        expiringSet.add( ob1 );

        // when
        fakeClock.forward( expiryPeriod.plusMillis( 1 ) );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        assertFalse( expiringSet.nonEmpty() );
        assertTrue( expiringSet.isEmpty() );
    }

    @Test
    void shouldRemoveObject()
    {
        // given
        expiringSet.add( ob1 );

        // when
        expiringSet.remove( ob1 );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        assertFalse( expiringSet.nonEmpty() );
        assertTrue( expiringSet.isEmpty() );
    }

    @Test
    void shouldExpireCorrectObject()
    {
        // given
        expiringSet.add( ob1 );
        fakeClock.forward( expiryPeriod.minusMillis( 1 ) );
        expiringSet.add( ob2 );

        // then
        hasObject( expiringSet, ob1 );
        hasObject( expiringSet, ob2 );

        // when
        fakeClock.forward( Duration.ofMillis( 1 ) );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        hasObject( expiringSet, ob2 );

        // when
        fakeClock.forward( expiryPeriod );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        doesNotHaveObject( expiringSet, ob2 );
        assertFalse( expiringSet.nonEmpty() );
        assertTrue( expiringSet.isEmpty() );
    }

    @Test
    void shouldClearContents()
    {
        // given
        expiringSet.add( ob1 );
        expiringSet.add( ob2 );

        // when
        expiringSet.clear();

        // then
        doesNotHaveObject( expiringSet, ob1 );
        doesNotHaveObject( expiringSet, ob2 );
        assertFalse( expiringSet.nonEmpty() );
        assertTrue( expiringSet.isEmpty() );
    }

    private static void hasObject( ExpiringSet<Object> expiringSet, Object ob1 )
    {
        assertTrue( expiringSet.contains( ob1 ) );
    }

    private static void doesNotHaveObject( ExpiringSet<Object> expiringSet, Object ob1 )
    {
        assertFalse( expiringSet.contains( ob1 ) );
    }
}
