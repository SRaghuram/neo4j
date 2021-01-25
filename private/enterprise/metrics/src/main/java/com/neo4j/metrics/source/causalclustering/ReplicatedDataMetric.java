/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;

import java.util.EnumMap;
import java.util.Map;

public class ReplicatedDataMetric implements ReplicatedDataMonitor
{
    private final Map<ReplicatedDataIdentifier,Integer> visibleDataSize = new EnumMap<>( ReplicatedDataIdentifier.class );
    private final Map<ReplicatedDataIdentifier,Integer> invisibleDataSize = new EnumMap<>( ReplicatedDataIdentifier.class );
    private static final Integer DEFAULT = 0;

    @Override
    public void setVisibleDataSize( ReplicatedDataIdentifier key, int size )
    {
        visibleDataSize.put( key, size );
    }

    @Override
    public void setInvisibleDataSize( ReplicatedDataIdentifier key, int size )
    {
        invisibleDataSize.put( key, size );
    }

    public Gauge<Integer> getVisibleDataSize( ReplicatedDataIdentifier key )
    {
        return () -> result( visibleDataSize, key );
    }

    public Gauge<Integer> getInvisibleDataSize( ReplicatedDataIdentifier key )
    {
        return () -> result( invisibleDataSize, key );
    }

    private Integer result( Map<ReplicatedDataIdentifier, Integer> map, ReplicatedDataIdentifier key )
    {
        Integer result = map.get( key );
        if ( result == null )
        {
            return DEFAULT;
        }
        else
        {
            return result;
        }
    }
}
