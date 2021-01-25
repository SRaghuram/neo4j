/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MultiTimeout implements Timeout
{
    public interface TimeoutKey
    {
        String name();
    }

    private final Map<Enum, Timeout> timeouts = new HashMap<>(  );
    private final Supplier<Enum> selector;

    public MultiTimeout( Supplier<Enum> selector )
    {
        this.selector = selector;
    }

    public MultiTimeout addTimeout( Enum key, Timeout timeout )
    {
        timeouts.put( key, timeout );
        return this;
    }

    @Override
    public Delay next()
    {
        return timeouts.get( selector.get() ).next();
    }
}
