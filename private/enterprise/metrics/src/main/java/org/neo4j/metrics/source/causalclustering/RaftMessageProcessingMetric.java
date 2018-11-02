/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.RaftMessageProcessingMonitor;
import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class RaftMessageProcessingMetric implements RaftMessageProcessingMonitor
{
    private final AtomicLong delay = new AtomicLong( 0 );
    private final Timer timer;
    private final Map<RaftMessages.Type,Timer> typeTimers = new EnumMap<>( RaftMessages.Type.class );

    public static RaftMessageProcessingMetric create()
    {
        return new RaftMessageProcessingMetric( Timer::new );
    }

    public static RaftMessageProcessingMetric createUsing( Supplier<Reservoir> reservoir )
    {
        return new RaftMessageProcessingMetric( () -> new Timer( reservoir.get() ) );
    }

    private RaftMessageProcessingMetric( Supplier<Timer> timerFactory )
    {
        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            typeTimers.put( type, timerFactory.get() );
        }
        this.timer = timerFactory.get();
    }

    public long delay()
    {
        return delay.get();
    }

    @Override
    public void setDelay( Duration delay )
    {
        this.delay.set( delay.toMillis() );
    }

    public Timer timer()
    {
        return timer;
    }

    public Timer timer( RaftMessages.Type type )
    {
        return typeTimers.get( type );
    }

    @Override
    public void updateTimer( RaftMessages.Type type, Duration duration )
    {
        long nanos = duration.toNanos();
        timer.update( nanos, TimeUnit.NANOSECONDS );
        typeTimers.get( type ).update( nanos, TimeUnit.NANOSECONDS );
    }
}
