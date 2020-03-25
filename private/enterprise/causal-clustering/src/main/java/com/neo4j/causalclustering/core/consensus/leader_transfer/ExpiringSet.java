/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExpiringSet<T>
{
    private final long expiryTime;
    private final Clock clock;
    private final Map<T,Long> elements = new HashMap<>();

    public ExpiringSet( long expiryTime, Clock clock )
    {
        this.expiryTime = expiryTime;
        this.clock = clock;
    }

    private void update()
    {
        elements.entrySet().removeIf( entry -> (entry.getValue() + expiryTime) < clock.millis() );
    }

    public void add( T element )
    {
        var timeOfInsertion = clock.millis();
        this.elements.put( element, timeOfInsertion );
    }

    public void remove( T element )
    {
        this.elements.remove( element );
    }

    public void clear()
    {
        this.elements.clear();
    }

    boolean contains( T suspended )
    {
        return currentElements().contains( suspended );
    }

    public boolean nonEmpty()
    {
        return !isEmpty();
    }

    public boolean isEmpty()
    {
        return currentElements().isEmpty();
    }

    private Set<T> currentElements()
    {
        update();
        return elements.keySet();
    }
}
