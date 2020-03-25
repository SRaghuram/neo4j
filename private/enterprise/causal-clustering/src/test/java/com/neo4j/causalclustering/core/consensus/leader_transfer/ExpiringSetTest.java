/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpiringSetTest
{
    private static final long TIMEOUT_MS = 1_000;
    private final FakeClock fakeClock = Clocks.fakeClock();
    private final ExpiringSet<Object> expiringSet = new ExpiringSet<>( TIMEOUT_MS, fakeClock );
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
        fakeClock.forward( TIMEOUT_MS + 1, TimeUnit.MILLISECONDS );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        assertFalse( expiringSet.nonEmpty() );
        assertTrue( expiringSet.isEmpty() );
    }

    @Test
    void shouldRemoveMember()
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
    void shouldExpireCorrectMember()
    {
        // given
        expiringSet.add( ob1 );
        fakeClock.forward( TIMEOUT_MS, TimeUnit.MILLISECONDS );
        expiringSet.add( ob2 );

        // then
        hasObject( expiringSet, ob1 );
        hasObject( expiringSet, ob2 );

        // when
        fakeClock.forward( 1, TimeUnit.MILLISECONDS );

        // then
        doesNotHaveObject( expiringSet, ob1 );
        hasObject( expiringSet, ob2 );

        // when
        fakeClock.forward( TIMEOUT_MS, TimeUnit.MILLISECONDS );

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
