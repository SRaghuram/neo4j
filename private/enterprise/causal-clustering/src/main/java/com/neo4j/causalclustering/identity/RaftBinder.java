/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

public class RaftBinder implements Supplier<Optional<RaftId>>
{
    public interface Monitor
    {
        void waitingForCoreMembers( DatabaseId databaseId, int minimumCount );

        void waitingForBootstrap( DatabaseId databaseId );

        void bootstrapped( CoreSnapshot snapshot, DatabaseId databaseId, RaftId raftId );

        void boundToRaft( DatabaseId databaseId, RaftId raftId );
    }

    private final DatabaseId databaseId;
    private final SimpleStorage<RaftId> raftIdStorage;
    private final CoreTopologyService topologyService;
    private final RaftBootstrapper raftBootstrapper;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final int minCoreHosts;

    private RaftId raftId;

    public RaftBinder( DatabaseId databaseId, SimpleStorage<RaftId> raftIdStorage,
            CoreTopologyService topologyService, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
            Duration timeout, RaftBootstrapper raftBootstrapper, int minCoreHosts, Monitors monitors )
    {
        this.databaseId = databaseId;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.raftIdStorage = raftIdStorage;
        this.topologyService = topologyService;
        this.raftBootstrapper = raftBootstrapper;
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
    private boolean hostShouldBootstrapRaft( DatabaseCoreTopology coreTopology )
    {
        int memberCount = coreTopology.members().size();
        if ( memberCount < minCoreHosts )
        {
            monitor.waitingForCoreMembers( databaseId, minCoreHosts );
            return false;
        }
        else if ( !topologyService.canBootstrapRaftGroup( databaseId ) )
        {
            monitor.waitingForBootstrap( databaseId );
            return false;
        }
        else
        {
            return true;
        }
    }

    /**
     * The raft binding process tries to establish a common raft ID. If there is no common raft ID
     * then a single instance will eventually create one and publish it through the underlying topology service.
     *
     * @throws IOException If there is an issue with I/O.
     * @throws InterruptedException If the process gets interrupted.
     * @throws TimeoutException If the process times out.
     */
    public BoundState bindToRaft() throws Exception
    {
        if ( raftIdStorage.exists() )
        {
            // If raft id state exists, read it and verify that it corresponds to the database being started
            raftId = raftIdStorage.readState();
            if ( !Objects.equals( raftId.uuid(), databaseId.uuid() ) )
            {
                throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                        "This may indicate a previous DROP operation for %s did not complete.", raftId.uuid(), databaseId.uuid(), databaseId.name() ) );
            }
            publishRaftId( raftId );
            monitor.boundToRaft( databaseId, raftId );
            return new BoundState( raftId );
        }

        CoreSnapshot snapshot = null;
        DatabaseCoreTopology topology;
        long endTime = clock.millis() + timeout.toMillis();

        do
        {
            topology = topologyService.coreTopologyForDatabase( databaseId );

            if ( topology.raftId() != null )
            {
                raftId = topology.raftId();
                monitor.boundToRaft( databaseId, raftId );
            }
            else if ( hostShouldBootstrapRaft( topology ) )
            {
                raftId = RaftId.from( databaseId );
                snapshot = raftBootstrapper.bootstrap( topology.members().keySet() );
                monitor.bootstrapped( snapshot, databaseId, raftId );
                publishRaftId( raftId );
            }

            retryWaiter.apply();

        }
        while ( raftId == null && clock.millis() < endTime );

        if ( raftId == null )
        {
            throw new TimeoutException( format(
                    "Failed to join a cluster with members %s. Another member should have published " +
                    "a raftId but none was detected. Please restart the cluster.", topology ) );
        }

        raftIdStorage.writeState( raftId );
        return new BoundState( raftId, snapshot );
    }

    @Override
    public Optional<RaftId> get()
    {
        return Optional.ofNullable( raftId );
    }

    private void publishRaftId( RaftId localRaftId ) throws BindingException, InterruptedException
    {
        boolean success = topologyService.setRaftId( localRaftId, databaseId );
        if ( !success )
        {
            throw new BindingException( "Failed to publish: " + localRaftId );
        }
    }
}
