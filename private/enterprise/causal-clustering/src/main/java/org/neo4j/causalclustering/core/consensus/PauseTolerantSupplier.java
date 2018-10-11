/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.SystemNanoClock;

class PauseTolerantSupplier implements Supplier<Optional<TimestampedValue>>
{
    private final Log log;
    private final SystemNanoClock clock;
    private final LongSupplier valueSupplier;
    private static final long PAUSE_TIME = TimeUnit.MILLISECONDS.toNanos( 5 );

    PauseTolerantSupplier( LogProvider logProvider, SystemNanoClock clock, LongSupplier valueSupplier )
    {
        log = logProvider.getLog( PauseTolerantSupplier.class );
        this.clock = clock;
        this.valueSupplier = valueSupplier;
    }

    @Override
    public Optional<TimestampedValue> get()
    {
        long now = clock.nanos();
        TimestampedValue value = new TimestampedValue( valueSupplier.getAsLong(), now, TimeUnit.NANOSECONDS );
        long endTime = clock.nanos();
        if ( endTime - now <= PAUSE_TIME )
        {
            return Optional.of( value );
        }
        else
        {
            log.info( "Failed to update entry due to pause time being %d ns (tolerance is %d ns)\n", endTime - now, PAUSE_TIME );
            return Optional.empty();
        }
    }
}
