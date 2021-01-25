/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This is a custom set whose membership is designed to be transient, with members being "removed" after a given expiry time.
 * It is designed to performant on contains/isEmpty/nonEmpty checks, but expensive on add/remove calls.
 * <p>
 * A few things to note:
 * <p>
 * - The collection is *not* thread safe.
 * - An object is considered expired when the clock *equals* its expiry time, not when it exceeds it.
 * - All objects have the same expiry period, specified in the constructor of the collection.
 */
public class ExpiringSet<T>
{
    private static final long BEGINNING_OF_TIME = 0L;
    private final long expiryTimeMillis;
    private final Clock clock;
    private final Map<T,Long> elements = new HashMap<>();
    private T lastExpiringElement;
    private long lastExpiry = BEGINNING_OF_TIME;

    public ExpiringSet( Duration expiryTime, Clock clock )
    {
        this.expiryTimeMillis = expiryTime.toMillis();
        this.clock = clock;
    }

    public void add( T element )
    {
        var timeOfExpiry = clock.millis() + this.expiryTimeMillis;

        if ( timeOfExpiry > lastExpiry )
        {
            this.lastExpiringElement = element;
            this.lastExpiry = timeOfExpiry;
        }
        this.elements.put( element, timeOfExpiry );
        this.removeExpired();
    }

    public void remove( T element )
    {
        this.removeExpired();
        var removed = this.elements.remove( element );
        if ( removed == null )
        {
            return;
        }

        if ( Objects.equals( lastExpiringElement, element ) )
        {
            var newLatest = this.elements.entrySet().stream().max( Map.Entry.comparingByValue() );
            newLatest.ifPresentOrElse(
                    e -> setLastEntry( e.getKey(), e.getValue() ),
                    () -> setLastEntry( null, BEGINNING_OF_TIME )
            );
        }
    }

    public void clear()
    {
        this.elements.clear();
        this.lastExpiringElement = null;
        this.lastExpiry = BEGINNING_OF_TIME;
    }

    boolean contains( T element )
    {
        var expiresAt = elements.getOrDefault( element, Long.MIN_VALUE );
        return expiresAt > clock.millis();
    }

    public boolean nonEmpty()
    {
        return !isEmpty();
    }

    public boolean isEmpty()
    {
        return allElementsExpired( clock.millis() );
    }

    private boolean allElementsExpired( long currentMillis )
    {
        return currentMillis >= lastExpiry;
    }

    private void removeExpired()
    {
        var currentMillis = clock.millis();
        if ( allElementsExpired( currentMillis ) )
        {
            clear();
        }
        elements.entrySet().removeIf( entry -> currentMillis >= entry.getValue() );
    }

    private void setLastEntry( T element, long expiry )
    {
        this.lastExpiringElement = element;
        this.lastExpiry = expiry;
    }
}
