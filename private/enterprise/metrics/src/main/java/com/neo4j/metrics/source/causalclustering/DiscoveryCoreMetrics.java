/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.MetricRegistry;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;
import static com.neo4j.metrics.source.causalclustering.RaftCoreMetrics.CAUSAL_CLUSTERING_PREFIX;

@Documented( ".Discovery core metrics" )
public class DiscoveryCoreMetrics extends LifecycleAdapter
{
    @Documented( "Size of replicated data structures." )
    public static final String REPLICATED_DATA_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "discovery", "replicated_data" );
    @Documented( "Discovery cluster member size." )
    public static final String CLUSTER_MEMBERS_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "discovery", "cluster", "members" );
    @Documented( "Discovery cluster unreachable size." )
    public static final String CLUSTER_UNREACHABLE_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "discovery", "cluster", "unreachable" );
    @Documented( "Discovery cluster convergence." )
    public static final String CLUSTER_CONVERGED_TEMPLATE = name( CAUSAL_CLUSTERING_PREFIX, "discovery", "cluster", "converged" );

    private final ClusterSizeMetric discoveryClusterSizeMetric = new ClusterSizeMetric();
    private final ReplicatedDataMetric discoveryReplicatedDataMetric = new ReplicatedDataMetric();
    private final Monitors monitors;
    private final MetricRegistry registry;

    private final String clusterConverged;
    private final String clusterMembers;
    private final String clusterUnreachable;
    private final String replicatedData;

    public DiscoveryCoreMetrics( String globalMetricsPrefix, Monitors globalMonitors, MetricRegistry metricRegistry )
    {
        this.monitors = globalMonitors;
        this.registry = metricRegistry;
        this.clusterConverged = name( globalMetricsPrefix, CLUSTER_CONVERGED_TEMPLATE );
        this.clusterMembers = name( globalMetricsPrefix, CLUSTER_MEMBERS_TEMPLATE );
        this.clusterUnreachable = name( globalMetricsPrefix, CLUSTER_UNREACHABLE_TEMPLATE );
        this.replicatedData = name( globalMetricsPrefix, REPLICATED_DATA_TEMPLATE );
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( discoveryReplicatedDataMetric );
        monitors.addMonitorListener( discoveryClusterSizeMetric );

        registry.register( clusterConverged, discoveryClusterSizeMetric.converged() );
        registry.register( clusterMembers, discoveryClusterSizeMetric.members() );
        registry.register( clusterUnreachable, discoveryClusterSizeMetric.unreachable() );

        for ( ReplicatedDataIdentifier identifier : ReplicatedDataIdentifier.values() )
        {
            registry.register( discoveryReplicatedDataName( identifier, "visible" ), discoveryReplicatedDataMetric.getVisibleDataSize( identifier ) );
            registry.register( discoveryReplicatedDataName( identifier, "invisible" ), discoveryReplicatedDataMetric.getInvisibleDataSize( identifier ) );
        }
    }

    @Override
    public void stop()
    {
        registry.remove( CLUSTER_CONVERGED_TEMPLATE );
        registry.remove( CLUSTER_MEMBERS_TEMPLATE );
        registry.remove( CLUSTER_UNREACHABLE_TEMPLATE );

        for ( ReplicatedDataIdentifier identifier : ReplicatedDataIdentifier.values() )
        {
            registry.remove( discoveryReplicatedDataName( identifier, "visible" ) );
            registry.remove( discoveryReplicatedDataName( identifier, "invisible" ) );
        }

        monitors.removeMonitorListener( discoveryReplicatedDataMetric );
        monitors.removeMonitorListener( discoveryClusterSizeMetric );
    }

    private String discoveryReplicatedDataName( ReplicatedDataIdentifier identifier, String visibility )
    {
        return name( replicatedData, identifier.keyName().replace( '-', '_' ) + "." + visibility );
    }
}