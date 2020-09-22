/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Bolt metrics" )
public class BoltMetrics extends LifecycleAdapter
{
    private static final String BOLT_PREFIX = "bolt";

    @Documented( "The total number of Bolt sessions started since this instance started. This includes both " +
            "succeeded and failed sessions (deprecated, use connections_opened instead). (counter)" )
    private static final String SESSIONS_STARTED_TEMPLATE = name( BOLT_PREFIX, "sessions_started" );

    @Documented( "The total number of Bolt connections opened since this instance started. This includes both " +
            "succeeded and failed connections. (counter)" )
    private static final String CONNECTIONS_OPENED_TEMPLATE = name( BOLT_PREFIX, "connections_opened" );

    @Documented( "The total number of Bolt connections closed since this instance started. This includes both " +
            "properly and abnormally ended connections. (counter)" )
    private static final String CONNECTIONS_CLOSED_TEMPLATE = name( BOLT_PREFIX, "connections_closed" );

    @Documented( "The total number of Bolt connections currently being executed. (gauge)" )
    private static final String CONNECTIONS_RUNNING_TEMPLATE = name( BOLT_PREFIX, "connections_running" );

    @Documented( "The total number of Bolt connections sitting idle. (gauge)" )
    private static final String CONNECTIONS_IDLE_TEMPLATE = name( BOLT_PREFIX, "connections_idle" );

    @Documented( "The total number of messages received via Bolt since this instance started. (counter)" )
    private static final String MESSAGES_RECEIVED_TEMPLATE = name( BOLT_PREFIX, "messages_received" );

    @Documented( "The total number of messages that began processing since this instance started. This is different " +
            "from messages received in that this counter tracks how many of the received messages have" +
            "been taken on by a worker thread. (counter)" )
    private static final String MESSAGES_STARTED_TEMPLATE = name( BOLT_PREFIX, "messages_started" );

    @Documented( "The total number of messages that completed processing since this instance started. This includes " +
            "successful, failed and ignored Bolt messages. (counter)" )
    private static final String MESSAGES_DONE_TEMPLATE = name( BOLT_PREFIX, "messages_done" );

    @Documented( "The total number of messages that failed processing since this instance started. (counter)" )
    private static final String MESSAGES_FAILED_TEMPLATE = name( BOLT_PREFIX, "messages_failed" );

    @Documented( "The accumulated time messages have spent waiting for a worker thread. (counter)" )
    private static final String TOTAL_QUEUE_TIME_TEMPLATE = name( BOLT_PREFIX, "accumulated_queue_time" );

    @Documented( "The accumulated time worker threads have spent processing messages. (counter)" )
    private static final String TOTAL_PROCESSING_TIME_TEMPLATE = name( BOLT_PREFIX, "accumulated_processing_time" );

    private final String sessionsStarted;
    private final String connectionsOpened;
    private final String connectionsClosed;
    private final String connectionsRunning;
    private final String connectionsIdle;
    private final String messagesReceived;
    private final String messagesStarted;
    private final String messagesDone;
    private final String messagesFailed;
    private final String totalQueueTime;
    private final String totalProcessingTime;

    private final MetricsRegister registry;
    private final Monitors monitors;
    private final BoltMetricsMonitor boltMonitor = new BoltMetricsMonitor();

    public BoltMetrics( String metricsPrefix, MetricsRegister registry, Monitors monitors )
    {
        this.sessionsStarted = name( metricsPrefix, SESSIONS_STARTED_TEMPLATE );
        this.connectionsOpened = name( metricsPrefix, CONNECTIONS_OPENED_TEMPLATE );
        this.connectionsClosed = name( metricsPrefix, CONNECTIONS_CLOSED_TEMPLATE );
        this.connectionsRunning = name( metricsPrefix, CONNECTIONS_RUNNING_TEMPLATE );
        this.connectionsIdle = name( metricsPrefix, CONNECTIONS_IDLE_TEMPLATE );
        this.messagesReceived = name( metricsPrefix, MESSAGES_RECEIVED_TEMPLATE );
        this.messagesStarted = name( metricsPrefix, MESSAGES_STARTED_TEMPLATE );
        this.messagesDone = name( metricsPrefix, MESSAGES_DONE_TEMPLATE );
        this.messagesFailed = name( metricsPrefix, MESSAGES_FAILED_TEMPLATE );
        this.totalQueueTime = name( metricsPrefix, TOTAL_QUEUE_TIME_TEMPLATE );
        this.totalProcessingTime = name( metricsPrefix, TOTAL_PROCESSING_TIME_TEMPLATE );
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( boltMonitor );
        registry.register( sessionsStarted, () -> new MetricsCounter( boltMonitor.connectionsOpened::get ) );
        registry.register( connectionsOpened, () -> new MetricsCounter( boltMonitor.connectionsOpened::get ) );
        registry.register( connectionsClosed, () -> new MetricsCounter( boltMonitor.connectionsClosed::get ) );
        registry.register( connectionsRunning, () -> (Gauge<Long>) boltMonitor.connectionsActive::get );
        registry.register( connectionsIdle, () -> (Gauge<Long>) boltMonitor.connectionsIdle::get );
        registry.register( messagesReceived, () -> new MetricsCounter( boltMonitor.messagesReceived::get ) );
        registry.register( messagesStarted, () -> new MetricsCounter( boltMonitor.messagesStarted::get ) );
        registry.register( messagesDone, () -> new MetricsCounter( boltMonitor.messagesDone::get ) );
        registry.register( messagesFailed, () -> new MetricsCounter( boltMonitor.messagesFailed::get ) );
        registry.register( totalQueueTime, () -> new MetricsCounter( boltMonitor.queueTime::get ) );
        registry.register( totalProcessingTime, () -> new MetricsCounter( boltMonitor.processingTime::get ) );
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

    private static class BoltMetricsMonitor implements BoltConnectionMetricsMonitor
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
