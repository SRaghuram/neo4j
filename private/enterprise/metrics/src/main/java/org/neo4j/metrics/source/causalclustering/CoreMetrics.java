/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Core metrics" )
public class CoreMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.core";

    @Documented( "Append index of the RAFT log" )
    public static final String APPEND_INDEX = name( CAUSAL_CLUSTERING_PREFIX, "append_index" );
    @Documented( "Commit index of the RAFT log" )
    public static final String COMMIT_INDEX = name( CAUSAL_CLUSTERING_PREFIX, "commit_index" );
    @Documented( "RAFT Term of this server" )
    public static final String TERM = name( CAUSAL_CLUSTERING_PREFIX, "term" );
    @Documented( "Transaction retries" )
    public static final String TX_RETRIES = name( CAUSAL_CLUSTERING_PREFIX, "tx_retries" );
    @Documented( "Is this server the leader?" )
    public static final String IS_LEADER = name( CAUSAL_CLUSTERING_PREFIX, "is_leader" );
    @Documented( "In-flight cache total bytes" )
    public static final String TOTAL_BYTES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "total_bytes" );
    @Documented( "In-flight cache max bytes" )
    public static final String MAX_BYTES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_bytes" );
    @Documented( "In-flight cache element count" )
    public static final String ELEMENT_COUNT = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "element_count" );
    @Documented( "In-flight cache maximum elements" )
    public static final String MAX_ELEMENTS = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_elements" );
    @Documented( "In-flight cache hits" )
    public static final String HITS = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "hits" );
    @Documented( "In-flight cache misses" )
    public static final String MISSES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "misses" );
    @Documented( "Delay between RAFT message receive and process" )
    public static final String DELAY = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_delay" );
    @Documented( "Timer for RAFT message processing" )
    public static final String TIMER = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_timer" );
    @Documented( "Raft replication new request count" )
    public static final String REPLICATION_NEW = name( CAUSAL_CLUSTERING_PREFIX, "replication_new" );
    @Documented( "Raft replication attempt count" )
    public static final String REPLICATION_ATTEMPT = name( CAUSAL_CLUSTERING_PREFIX, "replication_attempt" );
    @Documented( "Raft Replication success count" )
    public static final String REPLICATION_SUCCESS = name( CAUSAL_CLUSTERING_PREFIX, "replication_success" );
    @Documented( "Raft Replication fail count" )
    public static final String REPLICATION_FAIL = name( CAUSAL_CLUSTERING_PREFIX, "replication_fail" );

    private Monitors monitors;
    private MetricRegistry registry;
    private Supplier<CoreMetaData> coreMetaData;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();
    private final RaftTermMetric raftTermMetric = new RaftTermMetric();
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();
    private final TxRetryMetric txRetryMetric = new TxRetryMetric();
    private final InFlightCacheMetric inFlightCacheMetric = new InFlightCacheMetric();
    private final RaftMessageProcessingMetric raftMessageProcessingMetric = RaftMessageProcessingMetric.create();
    private final ReplicationMetric replicationMetric = new ReplicationMetric();

    public CoreMetrics( Monitors monitors, MetricRegistry registry, Supplier<CoreMetaData> coreMetaData )
    {
        this.monitors = monitors;
        this.registry = registry;
        this.coreMetaData = coreMetaData;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( raftLogCommitIndexMetric );
        monitors.addMonitorListener( raftLogAppendIndexMetric );
        monitors.addMonitorListener( raftTermMetric );
        monitors.addMonitorListener( txPullRequestsMetric );
        monitors.addMonitorListener( txRetryMetric );
        monitors.addMonitorListener( inFlightCacheMetric );
        monitors.addMonitorListener( raftMessageProcessingMetric );
        monitors.addMonitorListener( replicationMetric );

        registry.register( COMMIT_INDEX, (Gauge<Long>) raftLogCommitIndexMetric::commitIndex );
        registry.register( APPEND_INDEX, (Gauge<Long>) raftLogAppendIndexMetric::appendIndex );
        registry.register( TERM, (Gauge<Long>) raftTermMetric::term );
        registry.register( TX_RETRIES, new MetricsCounter( txRetryMetric::transactionsRetries ) );
        registry.register( IS_LEADER, new LeaderGauge() );
        registry.register( TOTAL_BYTES, (Gauge<Long>) inFlightCacheMetric::getTotalBytes );
        registry.register( HITS, new MetricsCounter( inFlightCacheMetric::getHits ) );
        registry.register( MISSES, new MetricsCounter( inFlightCacheMetric::getMisses ) );
        registry.register( MAX_BYTES, (Gauge<Long>) inFlightCacheMetric::getMaxBytes );
        registry.register( MAX_ELEMENTS, (Gauge<Long>) inFlightCacheMetric::getMaxElements );
        registry.register( ELEMENT_COUNT, (Gauge<Long>) inFlightCacheMetric::getElementCount );
        registry.register( DELAY, (Gauge<Long>) raftMessageProcessingMetric::delay );
        registry.register( TIMER, raftMessageProcessingMetric.timer() );
        registry.register( REPLICATION_NEW, new MetricsCounter( replicationMetric::newReplicationCount ) );
        registry.register( REPLICATION_ATTEMPT, new MetricsCounter( replicationMetric::attemptCount ) );
        registry.register( REPLICATION_SUCCESS, new MetricsCounter( replicationMetric::successCount ) );
        registry.register( REPLICATION_FAIL, new MetricsCounter( replicationMetric::failCount ) );

        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            registry.register( messageTimerName( type ), raftMessageProcessingMetric.timer( type ) );
        }
    }

    @Override
    public void stop()
    {
        registry.remove( COMMIT_INDEX );
        registry.remove( APPEND_INDEX );
        registry.remove( TERM );
        registry.remove( TX_RETRIES );
        registry.remove( IS_LEADER );
        registry.remove( TOTAL_BYTES );
        registry.remove( HITS );
        registry.remove( MISSES );
        registry.remove( MAX_BYTES );
        registry.remove( MAX_ELEMENTS );
        registry.remove( ELEMENT_COUNT );
        registry.remove( DELAY );
        registry.remove( TIMER );
        registry.remove( REPLICATION_NEW );
        registry.remove( REPLICATION_ATTEMPT );
        registry.remove( REPLICATION_SUCCESS );
        registry.remove( REPLICATION_FAIL );

        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            registry.remove( messageTimerName( type ) );
        }

        monitors.removeMonitorListener( raftLogCommitIndexMetric );
        monitors.removeMonitorListener( raftLogAppendIndexMetric );
        monitors.removeMonitorListener( raftTermMetric );
        monitors.removeMonitorListener( txPullRequestsMetric );
        monitors.removeMonitorListener( txRetryMetric );
        monitors.removeMonitorListener( inFlightCacheMetric );
        monitors.removeMonitorListener( raftMessageProcessingMetric );
        monitors.removeMonitorListener( replicationMetric );
    }

    private String messageTimerName( RaftMessages.Type type )
    {
        return name( TIMER, type.name().toLowerCase() );
    }

    private class LeaderGauge implements Gauge<Integer>
    {
        @Override
        public Integer getValue()
        {
            return coreMetaData.get().isLeader() ? 1 : 0;
        }
    }
}
