/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.google.common.testing.FakeTicker;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShiroCaffeineCacheTest
{
    private ShiroCaffeineCache<Integer,String> cache;
    private FakeTicker fakeTicker;
    private final long TTL = 100;

    @BeforeEach
    void setUp()
    {
        fakeTicker = new FakeTicker();
        cache = new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, TTL, 5, true );
    }

    @Test
    void shouldFailToCreateAuthCacheForTTLZeroIfUsingTLL()
    {
        new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, 0, 5, false );
        var e = assertThrows( IllegalArgumentException.class, () -> new ShiroCaffeineCache<>( fakeTicker::read, Runnable::run, 0, 5, true ) );
        assertThat( e.getMessage(), containsString( "TTL must be larger than zero." ) );
    }

    @Test
    void shouldNotGetNonExistentValue()
    {
        assertThat( cache.get( 1 ), equalTo( null ) );
    }

    @Test
    void shouldPutAndGet()
    {
        cache.put( 1, "1" );
        assertThat( cache.get( 1 ), equalTo( "1" ) );
    }

    @Test
    void shouldNotReturnExpiredValueThroughPut()
    {
        assertNull( cache.put( 1, "first" ));
        assertThat( cache.put( 1, "second" ), equalTo( "first" ) );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        assertNull( cache.put( 1, "third" ) );
    }

    @Test
    void shouldRemove()
    {
        assertNull( cache.remove( 1 ) );
        cache.put( 1, "1" );
        assertThat( cache.remove( 1 ), equalTo( "1" ) );
    }

    @Test
    void shouldClear()
    {
        cache.put( 1, "1" );
        cache.put( 2, "2" );
        assertThat( cache.size(), equalTo( 2 ) );
        cache.clear();
        assertThat( cache.size(), equalTo( 0 ) );
    }

    @Test
    void shouldGetKeys()
    {
        cache.put( 1, "1" );
        cache.put( 2, "1" );
        cache.put( 3, "1" );
        assertThat( cache.keys(), containsInAnyOrder( 1, 2, 3 ) );
    }

    @Test
    void shouldGetValues()
    {
        cache.put( 1, "1" );
        cache.put( 2, "1" );
        cache.put( 3, "1" );
        assertThat( cache.values(), containsInAnyOrder( "1", "1", "1" ) );
    }

    @Test
    void shouldNotListExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.values(), containsInAnyOrder( "foo" ) );
    }

    @Test
    void shouldNotGetExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.get( 1 ), equalTo( null ) );
        assertThat( cache.get( 2 ), equalTo( "foo" ) );
    }

    @Test
    void shouldNotGetKeysForExpiredValues()
    {
        cache.put( 1, "1" );
        fakeTicker.advance( TTL + 1, MILLISECONDS );
        cache.put( 2, "foo" );

        assertThat( cache.keys(), containsInAnyOrder( 2 ) );
    }

    @Test
    void shouldRemoveIfExceededCapacity()
    {
        cache.put( 1, "one" );
        cache.put( 2, "two" );
        cache.put( 3, "three" );
        cache.put( 4, "four" );
        cache.put( 5, "five" );
        cache.put( 6, "six" );

        assertThat( cache.size(), equalTo( 5 ) );
    }

    @Test
    void shouldGetValueAfterTimePassed()
    {
        cache.put( 1, "foo" );
        fakeTicker.advance( TTL - 1, MILLISECONDS );
        assertThat( cache.get( 1 ), equalTo( "foo" ) );
    }
}
