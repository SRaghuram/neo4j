/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.iocontroller;

import java.io.Flushable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ObjLongConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Math.min;

public class ConfigurableIOController implements IOController
{
    private static final AtomicLongFieldUpdater<ConfigurableIOController> stateUpdater =
            AtomicLongFieldUpdater.newUpdater( ConfigurableIOController.class, "state" );

    private static final int NO_LIMIT = 0;
    private static final int QUANTUM_MILLIS = 100;
    private static final int TIME_BITS = 32;
    private static final long TIME_MASK = (1L << TIME_BITS) - 1;
    private static final int QUANTUMS_PER_SECOND = (int) (TimeUnit.SECONDS.toMillis( 1 ) / QUANTUM_MILLIS);

    private final ObjLongConsumer<Object> pauseNanos;
    private final SystemNanoClock clock;

    /**
     * Upper 32 bits is the "disabled counter", lower 32 bits is the "IOs per quantum" field.
     * The "disabled counter" is modified online in 2-increments, leaving the lowest bit for signalling when
     * the limiter disabled by configuration.
     */
    @SuppressWarnings( "unused" ) // Updated via stateUpdater
    private volatile long state;

    private final LongAdder externalIO = new LongAdder();

    public ConfigurableIOController( Config config, SystemNanoClock clock )
    {
        this( config, LockSupport::parkNanos, clock );
    }

    @VisibleForTesting
    ConfigurableIOController( Config config, ObjLongConsumer<Object> pauseNanos, SystemNanoClock clock )
    {
        this.pauseNanos = pauseNanos;
        this.clock = clock;
        Integer iops = config.get( GraphDatabaseSettings.check_point_iops_limit );
        updateConfiguration( iops );
        config.addListener( GraphDatabaseSettings.check_point_iops_limit,
                ( prev, update ) -> updateConfiguration( update ) );
    }

    private void updateConfiguration( Integer iops )
    {
        long oldState;
        long newState;
        if ( iops == null || iops < 1 )
        {
            do
            {
                oldState = stateUpdater.get( this );
                int disabledCounter = getDisabledCounter( oldState );
                disabledCounter |= 1; // Raise the "permanently disabled" bit.
                newState = composeState( disabledCounter, NO_LIMIT );
            }
            while ( !stateUpdater.compareAndSet( this, oldState, newState ) );
        }
        else
        {
            do
            {
                oldState = stateUpdater.get( this );
                int disabledCounter = getDisabledCounter( oldState );
                disabledCounter &= 0xFFFFFFFE; // Mask off "permanently disabled" bit.
                int iopq = iops / QUANTUMS_PER_SECOND;
                newState = composeState( disabledCounter, iopq );
            }
            while ( !stateUpdater.compareAndSet( this, oldState, newState ) );
        }
    }

    private static long composeState( int disabledCounter, int iopq )
    {
        return ((long) disabledCounter) << 32 | iopq;
    }

    private static int getIOPQ( long state )
    {
        return (int) (state & 0x00000000_FFFFFFFFL);
    }

    private static int getDisabledCounter( long state )
    {
        return (int) (state >>> 32);
    }

    // The stamp is in two 32-bit parts:
    // The high bits are the number of IOs performed since the last pause.
    // The low bits is the 32-bit timestamp in milliseconds (~25 day range) since the last pause.
    // We keep adding summing up the IOs until either a quantum elapses, or we've exhausted the IOs we're allowed in
    // this quantum. If we've exhausted our IOs, we pause for the rest of the quantum.
    // We don't make use of the Flushable at this point, because IOs from fsyncs have a high priority, so they
    // might jump the IO queue and cause delays for transaction log IOs. Further, fsync on some file systems also
    // flush the entire IO queue, which can cause delays on IO rate limited cloud machines.
    // We need the Flushable to be implemented in terms of sync_file_range before we can make use of it.
    // NOTE: The check-pointer IOPS setting is documented as being a "best effort" hint. We are making use of that
    // wording here, and not compensating for over-counted IOs. For instance, if we receive 2 quantums worth of IOs
    // in one quantum, we are not going to sleep for two quantums. The reason is that such compensation algorithms
    // can easily over-compensate, and end up sleeping a lot more than what makes sense for other rate limiting factors
    // in the system, thus wasting IO bandwidth. No, "best effort" here means that likely end up doing a little bit
    // more IO than what we've been configured to allow, but that's okay. If it's a problem, people can just reduce
    // their IOPS limit setting a bit more.

    @Override
    public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable, FlushEventOpportunity flushEvent )
    {
        flushEvent.reportIO( recentlyCompletedIOs );
        long state = stateUpdater.get( this );
        if ( getDisabledCounter( state ) > 0 )
        {
            return INITIAL_STAMP;
        }

        long now = clock.millis() & TIME_MASK;
        long then = previousStamp & TIME_MASK;

        if ( now - then > QUANTUM_MILLIS )
        {
            return now + (((long) recentlyCompletedIOs) << TIME_BITS);
        }

        long ioSum = (previousStamp >> TIME_BITS) + recentlyCompletedIOs + externalIO.sumThenReset();
        if ( ioSum >= getIOPQ( state ) )
        {
            long millisLeftInQuantum = min( QUANTUM_MILLIS, QUANTUM_MILLIS - (now - then) );
            pauseNanos.accept( this, TimeUnit.MILLISECONDS.toNanos( millisLeftInQuantum ) );
            flushEvent.throttle( millisLeftInQuantum );
            return currentTimeMillis() & TIME_MASK;
        }

        return then + (ioSum << TIME_BITS);
    }

    @Override
    public void disable()
    {
        long currentState;
        long newState;
        do
        {
            currentState = stateUpdater.get( this );
            // Increment by two to leave "permanently disabled bit" alone.
            int disabledCounter = getDisabledCounter( currentState ) + 2;
            newState = composeState( disabledCounter, getIOPQ( currentState ) );
        }
        while ( !stateUpdater.compareAndSet( this, currentState, newState ) );
    }

    @Override
    public void reportIO( int completedIOs )
    {
        externalIO.add( completedIOs );
    }

    @Override
    public void enable()
    {
        long currentState;
        long newState;
        do
        {
            currentState = stateUpdater.get( this );
            // Decrement by two to leave "permanently disabled bit" alone.
            int disabledCounter = getDisabledCounter( currentState ) - 2;
            newState = composeState( disabledCounter, getIOPQ( currentState ) );
        }
        while ( !stateUpdater.compareAndSet( this, currentState, newState ) );
    }

    @Override
    public boolean isEnabled()
    {
        return getDisabledCounter( state ) == 0;
    }

    private static long currentTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( System.nanoTime() );
    }
}
