/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.logging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.async.AsyncLogEvent;
import org.neo4j.logging.async.AsyncLogProvider;
import org.neo4j.util.concurrent.AsyncEventSender;
import org.neo4j.util.concurrent.AsyncEvents;

import static org.neo4j.util.FeatureToggles.flag;

public class AsyncLogging extends LifecycleAdapter implements Consumer<AsyncLogEvent>, AsyncEvents.Monitor
{
    private static final boolean ENABLED = flag( AsyncLogging.class, "ENABLED", true );

    public static LogProvider provider( LifeSupport life, LogProvider provider )
    {
        if ( ENABLED )
        {
            if ( provider instanceof NullLogProvider )
            {
                return provider;
            }
            return new AsyncLogProvider( life.add(
                    new AsyncLogging( provider.getLog( AsyncLogging.class ) ) ).eventSender(), provider );
        }
        else
        {
            return provider;
        }
    }

    private final Log metaLog;
    private final AsyncEvents<AsyncLogEvent> events;
    private long highCount;
    private ExecutorService executor;

    AsyncLogging( Log metaLog )
    {
        this.metaLog = metaLog;
        this.events = new AsyncEvents<>( this, this );
    }

    @Override
    public void accept( AsyncLogEvent event )
    {
        event.process();
    }

    @Override
    public void start()
    {
        highCount = 0;
        executor = Executors.newSingleThreadExecutor( new NamedThreadFactory( getClass().getSimpleName() ) );
        executor.submit( events );
        events.awaitStartup();
    }

    @Override
    public void stop()
    {
        events.shutdown();
        executor.shutdown();
        events.awaitTermination();
    }

    @Override
    public void eventCount( long count )
    {
        if ( metaLog.isDebugEnabled() )
        {
            if ( count > highCount )
            {
                metaLog.debug( "High mark increasing from %d to %d events", highCount, count );
                highCount = count;
            }
        }
    }

    AsyncEventSender<AsyncLogEvent> eventSender()
    {
        return events;
    }
}
