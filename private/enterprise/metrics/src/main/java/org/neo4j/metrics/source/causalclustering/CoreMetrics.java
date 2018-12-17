/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
    @Documented( "Append index of the RAFT log" )
    private final String appendIndex;
    @Documented( "Commit index of the RAFT log" )
    private final String commitIndex;
    @Documented( "RAFT Term of this server" )
    private final String term;
    @Documented( "Transaction retries" )
    private final String txRetries;
    @Documented( "Is this server the leader?" )
    private final String isLeader;
    @Documented( "In-flight cache total bytes" )
    private final String totalBytes;
    @Documented( "In-flight cache max bytes" )
    private final String maxBytes;
    @Documented( "In-flight cache element count" )
    private final String elementCount;
    @Documented( "In-flight cache maximum elements" )
    private final String maxElements;
    @Documented( "In-flight cache hits" )
    private final String hits;
    @Documented( "In-flight cache misses" )
    private final String misses;
    @Documented( "Delay between RAFT message receive and process" )
    private final String delay;
    @Documented( "Timer for RAFT message processing" )
    private final String timer;
    @Documented( "Raft replication new request count" )
    private final String replicationNew;
    @Documented( "Raft replication attempt count" )
    private final String replicationAttempt;
    @Documented( "Raft Replication success count" )
    private final String replicationSuccess;
    @Documented( "Raft Replication fail count" )
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
        this.appendIndex = name( metricsPrefix, "causal_clustering.core", "append_index" );
        this.commitIndex = name( metricsPrefix, "causal_clustering.core", "commit_index" );
        this.term = name( metricsPrefix, "causal_clustering.core", "term" );
        this.txRetries = name( metricsPrefix, "causal_clustering.core", "tx_retries" );
        this.isLeader = name( metricsPrefix, "causal_clustering.core", "is_leader" );
        this.totalBytes = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "total_bytes" );
        this.maxBytes = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "max_bytes" );
        this.elementCount = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "element_count" );
        this.maxElements = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "max_elements" );
        this.hits = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "hits" );
        this.misses = name( metricsPrefix, "in_flight_cache", "causal_clustering.core", "misses" );
        this.delay = name( metricsPrefix, "causal_clustering.core", "message_processing_delay" );
        this.timer = name( metricsPrefix, "causal_clustering.core", "message_processing_timer" );
        this.replicationNew = name( metricsPrefix, "causal_clustering.core", "replication_new" );
        this.replicationAttempt = name( metricsPrefix, "causal_clustering.core", "replication_attempt" );
        this.replicationSuccess = name( metricsPrefix, "causal_clustering.core", "replication_success" );
        this.replicationFail = name( metricsPrefix, "causal_clustering.core", "replication_fail" );
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
