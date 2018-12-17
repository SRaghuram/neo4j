/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Bolt metrics" )
public class BoltMetrics extends LifecycleAdapter
{
    @Documented( "The total number of Bolt sessions started since this instance started. This includes both " +
                 "succeeded and failed sessions (deprecated, use connections_opened instead)." )
    private final String sessionsStarted;

    @Documented( "The total number of Bolt connections opened since this instance started. This includes both " +
            "succeeded and failed connections." )
    private final String connectionsOpened;

    @Documented( "The total number of Bolt connections closed since this instance started. This includes both " +
            "properly and abnormally ended connections." )
    private final String connectionsClosed;

    @Documented( "The total number of Bolt connections currently being executed." )
    private final String connectionsRunning;

    @Documented( "The total number of Bolt connections sitting idle." )
    private final String connectionsIdle;

    @Documented( "The total number of messages received via Bolt since this instance started." )
    private final String messagesReceived;

    @Documented( "The total number of messages that began processing since this instance started. This is different " +
                 "from messages received in that this counter tracks how many of the received messages have" +
                 "been taken on by a worker thread." )
    private final String messagesStarted;

    @Documented( "The total number of messages that completed processing since this instance started. This includes " +
                 "successful, failed and ignored Bolt messages." )
    private final String messagesDone;

    @Documented( "The total number of messages that failed processing since this instance started." )
    private final String messagesFailed;

    @Documented( "The accumulated time messages have spent waiting for a worker thread." )
    private final String totalQueueTime;

    @Documented( "The accumulated time worker threads have spent processing messages." )
    private final String totalProcessingTime;

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final BoltMetricsMonitor boltMonitor = new BoltMetricsMonitor();

    public BoltMetrics( String metricsPrefix, MetricRegistry registry, Monitors monitors )
    {
        this.sessionsStarted = name( metricsPrefix, "bolt.sessions_started" );
        this.connectionsOpened = name( metricsPrefix, "bolt.connections_opened" );
        this.connectionsClosed = name( metricsPrefix, "bolt.connections_closed" );
        this.connectionsRunning = name( metricsPrefix, "bolt.connections_running" );
        this.connectionsIdle = name( metricsPrefix, "bolt.connections_idle" );
        this.messagesReceived = name( metricsPrefix, "bolt.messages_received" );
        this.messagesStarted = name( metricsPrefix, "bolt.messages_started" );
        this.messagesDone = name( metricsPrefix, "bolt.messages_done" );
        this.messagesFailed = name( metricsPrefix, "bolt.messages_failed" );
        this.totalQueueTime = name( metricsPrefix, "bolt.accumulated_queue_time" );
        this.totalProcessingTime = name( metricsPrefix, "bolt.accumulated_processing_time" );
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( boltMonitor );
        registry.register( sessionsStarted, new MetricsCounter( boltMonitor.connectionsOpened::get ) );
        registry.register( connectionsOpened, new MetricsCounter( boltMonitor.connectionsOpened::get ) );
        registry.register( connectionsClosed, new MetricsCounter( boltMonitor.connectionsClosed::get ) );
        registry.register( connectionsRunning, (Gauge<Long>) boltMonitor.connectionsActive::get );
        registry.register( connectionsIdle, (Gauge<Long>) boltMonitor.connectionsIdle::get );
        registry.register( messagesReceived, new MetricsCounter( boltMonitor.messagesReceived::get ) );
        registry.register( messagesStarted, new MetricsCounter( boltMonitor.messagesStarted::get ) );
        registry.register( messagesDone, new MetricsCounter( boltMonitor.messagesDone::get ) );
        registry.register( messagesFailed, new MetricsCounter( boltMonitor.messagesFailed::get ) );
        registry.register( totalQueueTime, new MetricsCounter( boltMonitor.queueTime::get ) );
        registry.register( totalProcessingTime, new MetricsCounter( boltMonitor.processingTime::get ) );
    }

    @Override
    public void stop()
    {
        registry.remove( sessionsStarted );
        registry.remove( connectionsOpened );
        registry.remove( connectionsClosed );
        registry.remove( connectionsIdle );
        registry.remove( connectionsRunning );
        registry.remove( messagesReceived );
        registry.remove( messagesStarted );
        registry.remove( messagesDone );
        registry.remove( messagesFailed );
        registry.remove( totalQueueTime );
        registry.remove( totalProcessingTime );
        monitors.removeMonitorListener( boltMonitor );
    }

    private class BoltMetricsMonitor implements BoltConnectionMetricsMonitor
    {
        final AtomicLong connectionsOpened = new AtomicLong();
        final AtomicLong connectionsClosed = new AtomicLong();

        final AtomicLong connectionsActive = new AtomicLong();
        final AtomicLong connectionsIdle = new AtomicLong();

        final AtomicLong messagesReceived = new AtomicLong();
        final AtomicLong messagesStarted = new AtomicLong();
        final AtomicLong messagesDone = new AtomicLong();
        final AtomicLong messagesFailed = new AtomicLong();

        // It will take about 300 million years of queue/processing time to overflow these
        // Even if we run a million processors concurrently, the instance would need to
        // run uninterrupted for three hundred years before the monitoring had a hiccup.
        final AtomicLong queueTime = new AtomicLong();
        final AtomicLong processingTime = new AtomicLong();

        @Override
        public void connectionOpened()
        {
            connectionsOpened.incrementAndGet();
            connectionsIdle.incrementAndGet();
        }

        @Override
        public void connectionActivated()
        {
            connectionsActive.incrementAndGet();
            connectionsIdle.decrementAndGet();
        }

        @Override
        public void connectionWaiting()
        {
            connectionsIdle.incrementAndGet();
            connectionsActive.decrementAndGet();
        }

        @Override
        public void messageReceived()
        {
            messagesReceived.incrementAndGet();
        }

        @Override
        public void messageProcessingStarted( long queueTime )
        {
            this.queueTime.addAndGet( queueTime );
            messagesStarted.incrementAndGet();
        }

        @Override
        public void messageProcessingCompleted( long processingTime )
        {
            this.processingTime.addAndGet( processingTime );
            messagesDone.incrementAndGet();
        }

        @Override
        public void messageProcessingFailed()
        {
            messagesFailed.incrementAndGet();
        }

        @Override
        public void connectionClosed()
        {
            connectionsClosed.incrementAndGet();
            connectionsIdle.decrementAndGet();
        }
    }
}
