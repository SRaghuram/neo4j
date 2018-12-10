/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.metric.MetricsCounter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Core metrics" )
public class CoreMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "causal_clustering.core";

    @Documented( "Append index of the RAFT log." )
    private static final String APPEND_INDEX_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "append_index" );
    @Documented( "Commit index of the RAFT log." )
    private static final String COMMIT_INDEX_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "commit_index" );
    @Documented( "RAFT Term of this server." )
    private static final String TERM_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "term" );
    @Documented( "Transaction retries." )
    private static final String TX_RETRIES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "tx_retries" );
    @Documented( "Is this server the leader?" )
    private static final String IS_LEADER_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "is_leader" );
    @Documented( "In-flight cache total bytes." )
    private static final String TOTAL_BYTES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "total_bytes" );
    @Documented( "In-flight cache max bytes." )
    private static final String MAX_BYTES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_bytes" );
    @Documented( "In-flight cache element count." )
    private static final String ELEMENT_COUNT_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "element_count" );
    @Documented( "In-flight cache maximum elements." )
    private static final String MAX_ELEMENTS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_elements" );
    @Documented( "In-flight cache hits." )
    private static final String HITS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "hits" );
    @Documented( "In-flight cache misses." )
    private static final String MISSES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "misses" );
    @Documented( "Delay between RAFT message receive and process." )
    private static final String DELAY_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_delay" );
    @Documented( "Timer for RAFT message processing." )
    private static final String TIMER_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_timer" );
    @Documented( "Raft replication new request count." )
    private static final String REPLICATION_NEW_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_new" );
    @Documented( "Raft replication attempt count." )
    private static final String REPLICATION_ATTEMPT_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_attempt" );
    @Documented( "Raft Replication success count." )
    private static final String REPLICATION_SUCCESS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_success" );
    @Documented( "Raft Replication fail count." )
    private static final String REPLICATION_FAIL_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_fail" );

    private final String appendIndex;
    private final String commitIndex;
    private final String term;
    private final String txRetries;
    private final String isLeader;
    private final String totalBytes;
    private final String maxBytes;
    private final String elementCount;
    private final String maxElements;
    private final String hits;
    private final String misses;
    private final String delay;
    private final String timer;
    private final String replicationNew;
    private final String replicationAttempt;
    private final String replicationSuccess;
    private final String replicationFail;

    private final Monitors monitors;
    private final MetricRegistry registry;
    private final Supplier<CoreMetaData> coreMetaData;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();
    private final RaftTermMetric raftTermMetric = new RaftTermMetric();
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();
    private final TxRetryMetric txRetryMetric = new TxRetryMetric();
    private final InFlightCacheMetric inFlightCacheMetric = new InFlightCacheMetric();
    private final RaftMessageProcessingMetric raftMessageProcessingMetric = RaftMessageProcessingMetric.create();
    private final ReplicationMetric replicationMetric = new ReplicationMetric();

    public CoreMetrics( String metricsPrefix, Monitors monitors, MetricRegistry registry, Supplier<CoreMetaData> coreMetaData )
    {
        this.appendIndex = name( metricsPrefix, APPEND_INDEX_TEMPLATE );
        this.commitIndex = name( metricsPrefix, COMMIT_INDEX_TEMPLATE );
        this.term = name( metricsPrefix, TERM_TEMPLATE );
        this.txRetries = name( metricsPrefix, TX_RETRIES_TEMPLATE );
        this.isLeader = name( metricsPrefix, IS_LEADER_TEMPLATE );
        this.totalBytes = name( metricsPrefix, TOTAL_BYTES_TEMPLATE );
        this.maxBytes = name( metricsPrefix, MAX_BYTES_TEMPLATE );
        this.elementCount = name( metricsPrefix, ELEMENT_COUNT_TEMPLATE );
        this.maxElements = name( metricsPrefix, MAX_ELEMENTS_TEMPLATE );
        this.hits = name( metricsPrefix, HITS_TEMPLATE );
        this.misses = name( metricsPrefix, MISSES_TEMPLATE );
        this.delay = name( metricsPrefix, DELAY_TEMPLATE );
        this.timer = name( metricsPrefix, TIMER_TEMPLATE );
        this.replicationNew = name( metricsPrefix, REPLICATION_NEW_TEMPLATE );
        this.replicationAttempt = name( metricsPrefix, REPLICATION_ATTEMPT_TEMPLATE );
        this.replicationSuccess = name( metricsPrefix, REPLICATION_SUCCESS_TEMPLATE );
        this.replicationFail = name( metricsPrefix, REPLICATION_FAIL_TEMPLATE );
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

        registry.register( commitIndex, (Gauge<Long>) raftLogCommitIndexMetric::commitIndex );
        registry.register( appendIndex, (Gauge<Long>) raftLogAppendIndexMetric::appendIndex );
        registry.register( term, (Gauge<Long>) raftTermMetric::term );
        registry.register( txRetries, new MetricsCounter( txRetryMetric::transactionsRetries ) );
        registry.register( isLeader, new LeaderGauge() );
        registry.register( totalBytes, (Gauge<Long>) inFlightCacheMetric::getTotalBytes );
        registry.register( hits, new MetricsCounter( inFlightCacheMetric::getHits ) );
        registry.register( misses, new MetricsCounter( inFlightCacheMetric::getMisses ) );
        registry.register( maxBytes, (Gauge<Long>) inFlightCacheMetric::getMaxBytes );
        registry.register( maxElements, (Gauge<Long>) inFlightCacheMetric::getMaxElements );
        registry.register( elementCount, (Gauge<Long>) inFlightCacheMetric::getElementCount );
        registry.register( delay, (Gauge<Long>) raftMessageProcessingMetric::delay );
        registry.register( timer, raftMessageProcessingMetric.timer() );
        registry.register( replicationNew, new MetricsCounter( replicationMetric::newReplicationCount ) );
        registry.register( replicationAttempt, new MetricsCounter( replicationMetric::attemptCount ) );
        registry.register( replicationSuccess, new MetricsCounter( replicationMetric::successCount ) );
        registry.register( replicationFail, new MetricsCounter( replicationMetric::failCount ) );

        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            registry.register( messageTimerName( type ), raftMessageProcessingMetric.timer( type ) );
        }
    }

    @Override
    public void stop()
    {
        registry.remove( commitIndex );
        registry.remove( appendIndex );
        registry.remove( term );
        registry.remove( txRetries );
        registry.remove( isLeader );
        registry.remove( totalBytes );
        registry.remove( hits );
        registry.remove( misses );
        registry.remove( maxBytes );
        registry.remove( maxElements );
        registry.remove( elementCount );
        registry.remove( delay );
        registry.remove( timer );
        registry.remove( replicationNew );
        registry.remove( replicationAttempt );
        registry.remove( replicationSuccess );
        registry.remove( replicationFail );

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
        return name( timer, type.name().toLowerCase() );
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
