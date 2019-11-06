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
import com.neo4j.causalclustering.discovery.DiscoveryTimeoutException;
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.internal.DatabaseLog;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

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
    private final ClusterSystemGraphDbmsModel systemGraph;
    private final RaftBootstrapper raftBootstrapper;
    private final MemberId myIdentity;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final int minCoreHosts;
    private final DatabaseLog log;

    private RaftId raftId;

    public RaftBinder( DatabaseId databaseId, MemberId myIdentity, SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService,
            ClusterSystemGraphDbmsModel systemGraph, Clock clock, ThrowingAction<InterruptedException> retryWaiter, Duration timeout,
            RaftBootstrapper raftBootstrapper, int minCoreHosts, Monitors monitors, DatabaseLogProvider logProvider )
    {
        this.databaseId = databaseId;
        this.myIdentity = myIdentity;
        this.systemGraph = systemGraph;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.raftIdStorage = raftIdStorage;
        this.topologyService = topologyService;
        this.raftBootstrapper = raftBootstrapper;
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeout = timeout;
        this.minCoreHosts = minCoreHosts;
        this.log = logProvider.getLog( getClass() );
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
     * @param databaseStartAborter the component used to check if a database was stopped/dropped while we were still trying to bind it
     */
    public BoundState bindToRaft( DatabaseStartAborter databaseStartAborter ) throws Exception
    {
        long endTime = clock.millis() + timeout.toMillis();

        if ( raftIdStorage.exists() )
        {
            return bindToRaftIdFromDisk( databaseStartAborter, endTime );
        }
        else
        {
            return bindToRaftGroupBootstrapper( databaseStartAborter, endTime );
        }
    }

    private BoundState bindToRaftIdFromDisk( DatabaseStartAborter databaseStartAborter, long endTime ) throws Exception
    {
        boolean shouldAbort;
        boolean publishSucceeded;

        // If raft id state exists, read it and verify that it corresponds to the database being started
        raftId = raftIdStorage.readState();
        if ( !Objects.equals( raftId.uuid(), databaseId.uuid() ) )
        {
            throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                    "This may indicate a previous DROP operation for %s did not complete.", raftId.uuid(), databaseId.uuid(), databaseId.name() ) );
        }

        do
        {
            publishSucceeded = publishRaftId( raftId, true );
            shouldAbort = databaseStartAborter.shouldAbort( databaseId );
        } while ( !publishSucceeded && !shouldAbort && clock.millis() < endTime );

        if ( shouldAbort )
        {
            throw new DatabaseStartAbortedException( databaseId );
        }
        else if ( !publishSucceeded )
        {
            throw new TimeoutException( format( "Failed to publish or observe the previously bound raftId %s. This, or another member must successfully " +
                    "publish the raft Id when starting the corresponding database. Please restart the cluster.", raftId ) );
        }

        monitor.boundToRaft( databaseId, raftId );
        return new BoundState( raftId );
    }

    private BoundState bindToRaftGroupBootstrapper( DatabaseStartAborter databaseStartAborter, long endTime ) throws Exception
    {
        CoreSnapshot snapshot = null;
        DatabaseCoreTopology topology;
        boolean shouldAbort;
        boolean publishSucceeded = false;

        do
        {
            topology = topologyService.coreTopologyForDatabase( databaseId );

            if ( topology.raftId() != null )
            {
                // Someone else bootstrapped, we're done!
                if ( databaseId.isSystemDatabase() )
                {
                    /* Because the bootstrapping instance might modify the seed and change
                       the store ID, other instances have to remove their seeds and perform
                       a store copy later in the startup. */
                    log.info( "Removing system database to force store copy" );
                    raftBootstrapper.removeStore();
                }
                raftId = topology.raftId();
                publishSucceeded = true;
                monitor.boundToRaft( databaseId, raftId );
            }
            else if ( databaseId.isSystemDatabase() || systemGraph.getInitialMembers( databaseId ).isEmpty() )
            {
                // Used for initial databases (system + default) in conjunction with new cluster formation,
                // or when cluster has been restored from a backup of the system database which defines other databases.
                log.info( "Trying bootstrap using discovery service method" );
                snapshot = tryBootstrapUsingDiscoveryService( topology );
            }
            else
            {
                Set<MemberId> initialMembers = systemGraph
                        .getInitialMembers( databaseId )
                        .stream()
                        .map( MemberId::new )
                        .collect( toSet() );

                StoreId storeId = systemGraph.getStoreId( databaseId );

                // Used for databases created during runtime in response to operator commands.
                log.info( "Trying bootstrap using initial members " + initialMembers + " and store ID " + storeId );
                snapshot = tryBootstrapUsingSystemDatabase( initialMembers, storeId );
            }

            if ( snapshot != null )
            {
                // Alright, we managed to bootstrap, we're done!
                raftId = RaftId.from( databaseId );
                publishSucceeded = publishRaftId( raftId, false );
            }

            shouldAbort = databaseStartAborter.shouldAbort( databaseId );
            retryWaiter.apply();
        }
        while ( !publishSucceeded && clock.millis() < endTime && !shouldAbort );

        if ( shouldAbort )
        {
            throw new DatabaseStartAbortedException( databaseId );
        }
        else if ( !publishSucceeded )
        {
            throw new TimeoutException( format( "Failed to join a raft group with id %s and members %s. Another member should have published " +
                    "the raftId but none was detected. Please restart the cluster.", raftId, topology ) );
        }

        monitor.bootstrapped( snapshot, databaseId, raftId );
        raftIdStorage.writeState( raftId );
        return new BoundState( raftId, snapshot );
    }

    private CoreSnapshot tryBootstrapUsingSystemDatabase( Set<MemberId> initialMembers, StoreId storeId )
    {
        if ( !initialMembers.contains( myIdentity ) )
        {
            return null;
        }
        return raftBootstrapper.bootstrap( initialMembers, storeId );
    }

    private CoreSnapshot tryBootstrapUsingDiscoveryService( DatabaseCoreTopology topology )
    {
        if ( !hostShouldBootstrapRaft( topology ) )
        {
            return null;
        }
        return raftBootstrapper.bootstrap( topology.members().keySet() );
    }

    @Override
    public Optional<RaftId> get()
    {
        return Optional.ofNullable( raftId );
    }

    /**
     * Publish the raft Id for the database being started to the discovery service,
     * as a signal to other members that this database has been bootstrapped
     *
     * @param localRaftId the raftId to be published
     * @param ifNotExists whether or not to treat an {@link PublishRaftIdOutcome#SUCCESSFUL_PUBLISH_BY_OTHER} outcome as a success or not
     * @return whether or not the publish operation succeeded, or failed and should be retried
     * @throws BindingException in the event of a non-retryable error.
     */
    private boolean publishRaftId( RaftId localRaftId, boolean ifNotExists ) throws BindingException
    {
        try
        {
            var outcome = topologyService.publishRaftId( localRaftId );
            switch ( outcome )
            {
                case SUCCESSFUL_PUBLISH_BY_OTHER:
                    return ifNotExists;
                case SUCCESSFUL_PUBLISH_BY_ME:
                    return true;
                default:
                    return false;
            }
        }
        catch ( DiscoveryTimeoutException e )
        {
            return false;
        }
        catch ( Throwable t )
        {
            throw new BindingException( format( "Failed to publish raftId %s", localRaftId ), t );
        }
    }
}
