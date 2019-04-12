/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.identity;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.CoreBootstrapper;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.DiscoveryTimeoutException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.monitoring.Monitors;

import static java.lang.String.format;

public class ClusterBinder implements Supplier<Optional<ClusterId>>
{
    public interface Monitor
    {
        void waitingForCoreMembers( int minimumCount );

        void waitingForBootstrap();

        void bootstrapped( CoreSnapshot snapshot, ClusterId clusterId );

        void boundToCluster( ClusterId clusterId );
    }

    private final SimpleStorage<ClusterId> clusterIdStorage;
    private final SimpleStorage<DatabaseName> dbNameStorage;
    private final CoreTopologyService topologyService;
    private final CoreBootstrapper coreBootstrapper;
    private final AvailabilityGuard availabilityGuard;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final String dbName;
    private final int minCoreHosts;

    private ClusterId clusterId;

    public ClusterBinder( SimpleStorage<ClusterId> clusterIdStorage, SimpleStorage<DatabaseName> dbNameStorage,
            CoreTopologyService topologyService, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
            Duration timeout, CoreBootstrapper coreBootstrapper, String dbName, AvailabilityGuard availabilityGuard,
            int minCoreHosts, Monitors monitors )
    {
        this.availabilityGuard = availabilityGuard;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.clusterIdStorage = clusterIdStorage;
        this.dbNameStorage = dbNameStorage;
        this.topologyService = topologyService;
        this.coreBootstrapper = coreBootstrapper;
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeout = timeout;
        this.dbName = dbName;
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
    public BoundState bindToCluster() throws Throwable
    {
        DatabaseName newName = new DatabaseName( dbName );

        dbNameStorage.writeOrVerify( newName, existing -> {
            if ( !newName.equals( existing ) )
            {
                throw new IllegalStateException( format( "Your configured database name has changed. Found %s but expected %s in %s.",
                        dbName, existing.name(), CausalClusteringSettings.database.name() ) );
            }
        } );

        long endTime = clock.millis() + timeout.toMillis();
        boolean shouldRetryPublish = false;

        if ( clusterIdStorage.exists() )
        {
            clusterId = clusterIdStorage.readState();
            do
            {
                shouldRetryPublish = !publishClusterId( clusterId, true );
            } while ( shouldRetryPublish && clock.millis() < endTime );
            monitor.boundToCluster( clusterId );
            return new BoundState( clusterId );
        }

        CoreSnapshot snapshot = null;
        CoreTopology topology;
        ClusterId proposedClusterId = new ClusterId( UUID.randomUUID() );

        do
        {
            topology = topologyService.localCoreServers();

            if ( topology.clusterId() != null )
            {
                clusterId = topology.clusterId();
                monitor.boundToCluster( clusterId );
            }
            else if ( hostShouldBootstrapCluster( topology ) )
            {
                clusterId = proposedClusterId;
                snapshot = coreBootstrapper.bootstrap( topology.members().keySet() );
                monitor.bootstrapped( snapshot, clusterId );
                shouldRetryPublish = !publishClusterId( clusterId, false );
            }

            if ( availabilityGuard.isShutdown() )
            {
                throw new DatabaseShutdownException( "Database has been shut down before cluster binding completed." );
            }
            else if ( clock.millis() >= endTime )
            {
                throw new TimeoutException( format(
                        "Failed to join a cluster with members %s. Another member should have published " +
                                "a clusterId but none was detected. Please restart the cluster.", topology ) );
            }

            retryWaiter.apply();

        } while ( clusterId == null || shouldRetryPublish );

        clusterIdStorage.writeState( clusterId );
        return new BoundState( clusterId, snapshot );
    }

    @Override
    public Optional<ClusterId> get()
    {
        return Optional.ofNullable( clusterId );
    }

    private boolean publishClusterId( ClusterId localClusterId, boolean dieOnFailure ) throws BindingException, InterruptedException
    {
        try
        {
            boolean success = topologyService.setClusterId( localClusterId, dbName );

            if ( !success && dieOnFailure )
            {
                throw new BindingException( "Failed to publish: " + localClusterId );
            }

            return success;
        }
        catch ( DiscoveryTimeoutException e )
        {
            //In the event of a timeout we want to retry
            return false;
        }
    }
}
