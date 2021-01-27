/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.neo4j.causalclustering.common.RaftMonitors;
import com.neo4j.causalclustering.core.consensus.CoreMetaData;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.metrics.metric.MetricsCounter;
import com.neo4j.metrics.metric.MetricsRegister;
import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

import java.util.function.Supplier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.service.ServiceProvider;

import static com.codahale.metrics.MetricRegistry.name;

@ServiceProvider
@Documented( ".Raft core metrics" )
public class RaftCoreMetrics extends Metrics
{
    static final String CAUSAL_CLUSTERING_PREFIX = "causal_clustering.core";

    @Documented( "Append index of the RAFT log. (gauge)" )
    private static final String APPEND_INDEX_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "append_index" );
    @Documented( "Commit index of the RAFT log. (gauge)" )
    private static final String COMMIT_INDEX_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "commit_index" );
    @Documented( "Applied index of the RAFT log. (gauge)" )
    private static final String APPLIED_INDEX_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "applied_index" );
    @Documented( "RAFT Term of this server. (gauge)" )
    private static final String TERM_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "term" );
    @Documented( "Transaction retries. (counter)" )
    private static final String TX_RETRIES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "tx_retries" );
    @Documented( "Is this server the leader? (gauge)" )
    private static final String IS_LEADER_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "is_leader" );
    @Documented( "In-flight cache total bytes. (gauge)" )
    private static final String TOTAL_BYTES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "total_bytes" );
    @Documented( "In-flight cache max bytes. (gauge)" )
    private static final String MAX_BYTES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_bytes" );
    @Documented( "In-flight cache element count. (gauge)" )
    private static final String ELEMENT_COUNT_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "element_count" );
    @Documented( "In-flight cache maximum elements. (gauge)" )
    private static final String MAX_ELEMENTS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_elements" );
    @Documented( "In-flight cache hits. (counter)" )
    private static final String HITS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "hits" );
    @Documented( "In-flight cache misses. (counter)" )
    private static final String MISSES_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "misses" );
    @Documented( "Delay between RAFT message receive and process. (gauge)" )
    private static final String DELAY_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_delay" );
    @Documented( "Timer for RAFT message processing. (counter, histogram)" )
    private static final String TIMER_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "message_processing_timer" );
    @Documented( "Raft replication new request count. (counter)" )
    private static final String REPLICATION_NEW_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_new" );
    @Documented( "Raft replication attempt count. (counter)" )
    private static final String REPLICATION_ATTEMPT_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_attempt" );
    @Documented( "Raft Replication fail count. (counter)" )
    private static final String REPLICATION_FAIL_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_fail" );
    @Documented( "Raft Replication maybe count. (counter)" )
    private static final String REPLICATION_MAYBE_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_maybe" );
    @Documented( "Raft Replication success count. (counter)" )
    private static final String REPLICATION_SUCCESS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "replication_success" );
    @Documented( "Time elapsed since last message from leader in milliseconds. (gauge)" )
    public static final String LAST_LEADER_MESSAGE_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "last_leader_message" );

    private final String appendIndex;
    private final String commitIndex;
    private final String appliedIndex;
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
    private final String replicationFail;
    private final String replicationMaybe;
    private final String replicationSuccess;
    private final String lastLeaderMessage;

    private final RaftMonitors monitors;
    private final MetricsRegister registry;
    private final Supplier<CoreMetaData> coreMetaData;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();
    private final RaftLogAppliedIndexMetric raftLogAppliedIndexMetric = new RaftLogAppliedIndexMetric();
    private final RaftTermMetric raftTermMetric = new RaftTermMetric();
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();
    private final TxRetryMetric txRetryMetric = new TxRetryMetric();
    private final InFlightCacheMetric inFlightCacheMetric = new InFlightCacheMetric();
    private final RaftMessageProcessingMetric raftMessageProcessingMetric = RaftMessageProcessingMetric.create();
    private final ReplicationMetric replicationMetric = new ReplicationMetric();
    private final LastLeaderMessageMetric lastLeaderMessageMetric;

    /**
     * Only for generating documentation. The metrics documentation is generated through
     * service loading which requires a zero-argument constructor.
     */
    public RaftCoreMetrics()
    {
        this( "", null, null, null );
    }

    public RaftCoreMetrics( String metricsPrefix, RaftMonitors monitors, MetricsRegister registry, Supplier<CoreMetaData> coreMetaData )
    {
        super( MetricGroup.CAUSAL_CLUSTERING );
        this.appendIndex = name( metricsPrefix, APPEND_INDEX_TEMPLATE );
        this.commitIndex = name( metricsPrefix, COMMIT_INDEX_TEMPLATE );
        this.appliedIndex = name( metricsPrefix, APPLIED_INDEX_TEMPLATE );
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
        this.replicationFail = name( metricsPrefix, REPLICATION_FAIL_TEMPLATE );
        this.replicationMaybe = name( metricsPrefix, REPLICATION_MAYBE_TEMPLATE );
        this.replicationSuccess = name( metricsPrefix, REPLICATION_SUCCESS_TEMPLATE );
        this.lastLeaderMessage = name( metricsPrefix, LAST_LEADER_MESSAGE_TEMPLATE );
        this.monitors = monitors;
        this.registry = registry;
        this.coreMetaData = coreMetaData;

        this.lastLeaderMessageMetric =  new LastLeaderMessageMetric( coreMetaData );
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( raftLogCommitIndexMetric );
        monitors.addMonitorListener( raftLogAppendIndexMetric );
        monitors.addMonitorListener( raftLogAppliedIndexMetric );
        monitors.addMonitorListener( raftTermMetric );
        monitors.addMonitorListener( txPullRequestsMetric );
        monitors.addMonitorListener( txRetryMetric );
        monitors.addMonitorListener( inFlightCacheMetric );
        monitors.addMonitorListener( raftMessageProcessingMetric );
        monitors.addMonitorListener( replicationMetric );
        monitors.addMonitorListener( lastLeaderMessageMetric );

        registry.register( commitIndex, () -> (Gauge<Long>) raftLogCommitIndexMetric::commitIndex );
        registry.register( appendIndex, () -> (Gauge<Long>) raftLogAppendIndexMetric::appendIndex );
        registry.register( appliedIndex, () -> (Gauge<Long>) raftLogAppliedIndexMetric::appliedIndex );
        registry.register( term, () -> (Gauge<Long>) raftTermMetric::term );
        registry.register( txRetries, () -> new MetricsCounter( txRetryMetric::transactionsRetries ) );
        registry.register( isLeader, () -> new LeaderGauge() );
        registry.register( totalBytes, () -> (Gauge<Long>) inFlightCacheMetric::getTotalBytes );
        registry.register( hits, () -> new MetricsCounter( inFlightCacheMetric::getHits ) );
        registry.register( misses, () -> new MetricsCounter( inFlightCacheMetric::getMisses ) );
        registry.register( maxBytes, () -> (Gauge<Long>) inFlightCacheMetric::getMaxBytes );
        registry.register( maxElements, () -> (Gauge<Long>) inFlightCacheMetric::getMaxElements );
        registry.register( elementCount, () -> (Gauge<Long>) inFlightCacheMetric::getElementCount );
        registry.register( delay, () -> (Gauge<Long>) raftMessageProcessingMetric::delay );
        registry.register( timer, () -> raftMessageProcessingMetric.timer() );
        registry.register( replicationNew, () -> new MetricsCounter( replicationMetric::newReplicationCount ) );
        registry.register( replicationAttempt, () -> new MetricsCounter( replicationMetric::attemptCount ) );
        registry.register( replicationFail, () -> new MetricsCounter( replicationMetric::failCount ) );
        registry.register( replicationMaybe, () -> new MetricsCounter( replicationMetric::maybeCount ) );
        registry.register( replicationSuccess, () -> new MetricsCounter( replicationMetric::successCount ) );
        registry.register( lastLeaderMessage, () -> lastLeaderMessageMetric );

        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            registry.register( messageTimerName( type ), () -> raftMessageProcessingMetric.timer( type ) );
        }
    }

    @Override
    public void stop()
    {
        registry.remove( commitIndex );
        registry.remove( appendIndex );
        registry.remove( appliedIndex );
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
        registry.remove( replicationFail );
        registry.remove( replicationMaybe );
        registry.remove( replicationSuccess );
        registry.remove( lastLeaderMessage );

        for ( RaftMessages.Type type : RaftMessages.Type.values() )
        {
            registry.remove( messageTimerName( type ) );
        }

        monitors.removeMonitorListener( raftLogCommitIndexMetric );
        monitors.removeMonitorListener( raftLogAppendIndexMetric );
        monitors.removeMonitorListener( raftLogAppliedIndexMetric );
        monitors.removeMonitorListener( raftTermMetric );
        monitors.removeMonitorListener( txPullRequestsMetric );
        monitors.removeMonitorListener( txRetryMetric );
        monitors.removeMonitorListener( inFlightCacheMetric );
        monitors.removeMonitorListener( raftMessageProcessingMetric );
        monitors.removeMonitorListener( replicationMetric );
        monitors.removeMonitorListener( lastLeaderMessageMetric );
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
