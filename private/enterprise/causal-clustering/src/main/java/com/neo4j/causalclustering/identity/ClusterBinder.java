/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.CoreBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.CoreTopologyService;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

public class ClusterBinder implements Supplier<Optional<ClusterId>>
{
    public interface Monitor
    {
        void waitingForCoreMembers( int minimumCount );

        void waitingForBootstrap();

        void bootstrapped( Map<DatabaseId,CoreSnapshot> snapshots, ClusterId clusterId );

        void boundToCluster( ClusterId clusterId );
    }

    private final DatabaseId databaseId;
    private final SimpleStorage<ClusterId> clusterIdStorage;
    private final CoreTopologyService topologyService;
    private final CoreBootstrapper coreBootstrapper;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final int minCoreHosts;

    private ClusterId clusterId;

    public ClusterBinder( DatabaseId databaseId, SimpleStorage<ClusterId> clusterIdStorage,
            CoreTopologyService topologyService, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
            Duration timeout, CoreBootstrapper coreBootstrapper, int minCoreHosts, Monitors monitors )
    {
        this.databaseId = databaseId;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.clusterIdStorage = clusterIdStorage;
        this.topologyService = topologyService;
        this.coreBootstrapper = coreBootstrapper;
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeout = timeout;
        this.minCoreHosts = minCoreHosts;
    }

    /**
     * This method verifies if the local topology being returned by the discovery service is a viable cluster
     * and should be bootstrapped by this host.
     *
     * If true, then a) the topology is sufficiently large to form a cluster; & b) this host can bootstrap for
     * its configured database.
     *
     * @param coreTopology the present state of the local topology, as reported by the discovery service.
     * @return Whether or not coreTopology, in its current state, can form a viable cluster
     */
    private boolean hostShouldBootstrapCluster( CoreTopology coreTopology )
    {
        int memberCount = coreTopology.members().size();
        if ( memberCount < minCoreHosts )
        {
            monitor.waitingForCoreMembers( minCoreHosts );
            return false;
        }
        else if ( !coreTopology.canBeBootstrapped() )
        {
            monitor.waitingForBootstrap();
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * The cluster binding process tries to establish a common cluster ID. If there is no common cluster ID
     * then a single instance will eventually create one and publish it through the underlying topology service.
     *
     * @throws IOException If there is an issue with I/O.
     * @throws InterruptedException If the process gets interrupted.
     * @throws TimeoutException If the process times out.
     */
    public BoundState bindToCluster() throws Exception
    {
        if ( clusterIdStorage.exists() )
        {
            clusterId = clusterIdStorage.readState();
            publishClusterId( clusterId );
            monitor.boundToCluster( clusterId );
            return new BoundState( clusterId );
        }

        Map<DatabaseId,CoreSnapshot> snapshots = Collections.emptyMap();
        CoreTopology topology;
        long endTime = clock.millis() + timeout.toMillis();

        do
        {
            topology = topologyService.coreTopologyForDatabase( databaseId );

            if ( topology.clusterId() != null )
            {
                clusterId = topology.clusterId();
                monitor.boundToCluster( clusterId );
            }
            else if ( hostShouldBootstrapCluster( topology ) )
            {
                clusterId = new ClusterId( UUID.randomUUID() );
                snapshots = coreBootstrapper.bootstrap( topology.members().keySet() );
                monitor.bootstrapped( snapshots, clusterId );
                publishClusterId( clusterId );
            }

            retryWaiter.apply();

        }
        while ( clusterId == null && clock.millis() < endTime );

        if ( clusterId == null )
        {
            throw new TimeoutException( format(
                    "Failed to join a cluster with members %s. Another member should have published " +
                    "a clusterId but none was detected. Please restart the cluster.", topology ) );
        }

        clusterIdStorage.writeState( clusterId );
        return new BoundState( clusterId, snapshots );
    }

    @Override
    public Optional<ClusterId> get()
    {
        return Optional.ofNullable( clusterId );
    }

    private void publishClusterId( ClusterId localClusterId ) throws BindingException, InterruptedException
    {
        boolean success = topologyService.setClusterId( localClusterId, databaseId );
        if ( !success )
        {
            throw new BindingException( "Failed to publish: " + localClusterId );
        }
    }
}
