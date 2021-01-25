/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness;

import com.neo4j.harness.internal.CausalClusterInProcessBuilder;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.test.ports.PortAuthority;


public class PortAuthorityPortPickingStrategy implements CausalClusterInProcessBuilder.PortPickingStrategy
{
    Map<Integer, Integer> cache = new HashMap<>();

    @Override
    public int port( int offset, int id )
    {
        int key = offset + id;
        if ( ! cache.containsKey( key ) )
        {
            cache.put( key, PortAuthority.allocatePort() );
        }

        return cache.get( key );
    }
}

