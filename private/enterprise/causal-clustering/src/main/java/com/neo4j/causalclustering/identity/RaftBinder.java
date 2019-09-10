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
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;
import com.neo4j.dbms.DatabaseStartAborter;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.internal.CappedLogger;
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
        void waitingForCoreMembers( NamedDatabaseId namedDatabaseId, int minimumCount );

        void waitingForBootstrap( NamedDatabaseId namedDatabaseId );

        void bootstrapped( CoreSnapshot snapshot, NamedDatabaseId namedDatabaseId, RaftId raftId );

        void boundToRaft( NamedDatabaseId namedDatabaseId, RaftId raftId );
    }

    private final NamedDatabaseId namedDatabaseId;
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
    private final BootstrapDatabaseLogger bootstrapDbLogger;

    private RaftId raftId;

    public RaftBinder( NamedDatabaseId namedDatabaseId, MemberId myIdentity, SimpleStorage<RaftId> raftIdStorage, CoreTopologyService topologyService,
            ClusterSystemGraphDbmsModel systemGraph, Clock clock, ThrowingAction<InterruptedException> retryWaiter, Duration timeout,
            RaftBootstrapper raftBootstrapper, int minCoreHosts, Monitors monitors, DatabaseLogProvider logProvider )
    {
        this.namedDatabaseId = namedDatabaseId;
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
        this.bootstrapDbLogger = new BootstrapDatabaseLogger( logProvider.getLog( getClass() ), clock );
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
            monitor.waitingForCoreMembers( namedDatabaseId, minCoreHosts );
            return false;
        }
        else if ( !topologyService.canBootstrapRaftGroup( namedDatabaseId ) )
        {
            monitor.waitingForBootstrap( namedDatabaseId );
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
        var bindingConditions = new BindingConditions( databaseStartAborter, clock, timeout );

        if ( raftIdStorage.exists() )
        {
            return bindToRaftIdFromDisk( bindingConditions );
        }
        else
        {
            return bindToRaftGroupBootstrapper( bindingConditions );
        }
    }

    private BoundState bindToRaftIdFromDisk( BindingConditions bindingConditions ) throws Exception
    {
        boolean publishSucceeded;

        // If raft id state exists, read it and verify that it corresponds to the database being started
        raftId = raftIdStorage.readState();
        if ( !Objects.equals( raftId.uuid(), namedDatabaseId.databaseId().uuid() ) )
        {
            throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                    "This may indicate a previous DROP operation for %s did not complete.",
                    raftId.uuid(), namedDatabaseId.databaseId().uuid(), namedDatabaseId.name() ) );
        }

        do
        {
            publishSucceeded = publishRaftId( raftId, true );
            retryWaiter.apply();
        } while ( !publishSucceeded && bindingConditions.shouldContinuePublishing( namedDatabaseId ) );

        monitor.boundToRaft( namedDatabaseId, raftId );
        return new BoundState( raftId );
    }

    private BoundState bindToRaftGroupBootstrapper( BindingConditions bindingConditions ) throws Exception
    {
        CoreSnapshot snapshot;
        DatabaseCoreTopology topology;
        boolean bound;

        do
        {
            snapshot = null;
            topology = topologyService.coreTopologyForDatabase( namedDatabaseId );

            if ( topology.raftId() != null )
            {
                // Someone else bootstrapped, we're done!
                if ( namedDatabaseId.isSystemDatabase() )
                {
                    /* Because the bootstrapping instance might modify the seed and change
                       the store ID, other instances have to remove their seeds and perform
                       a store copy later in the startup. */
                    bootstrapDbLogger.logRemoveSystemDatabase();
                    raftBootstrapper.removeStore();
                }
                raftId = topology.raftId();
                bound = true;
                monitor.boundToRaft( namedDatabaseId, raftId );
            }
            else if ( namedDatabaseId.isSystemDatabase() || systemGraph.getInitialMembers( namedDatabaseId ).isEmpty() )
            {
                // Used for initial databases (system + default) in conjunction with new cluster formation,
                // or when cluster has been restored from a backup of the system database which defines other databases.
                bootstrapDbLogger.logBootstrapAttemptWithDiscoveryService();
                snapshot = tryBootstrapUsingDiscoveryService( topology );
                bound = publishBootstrapper( namedDatabaseId, snapshot, bindingConditions, false );
            }
            else
            {
                Set<MemberId> initialMembers = systemGraph.getInitialMembers( namedDatabaseId ).stream().map( MemberId::new ).collect( toSet() );

                StoreId storeId = systemGraph.getStoreId( namedDatabaseId );

                // Used for databases created during runtime in response to operator commands.
                bootstrapDbLogger.logBootstrapWithInitialMembersAndStoreID( initialMembers, storeId );
                snapshot = tryBootstrapUsingSystemDatabase( initialMembers, storeId );
                bound = publishBootstrapper( namedDatabaseId, snapshot, bindingConditions, true );
            }

            retryWaiter.apply();
        }
        while ( !bound && bindingConditions.shouldContinueBinding( namedDatabaseId, topology ) );

        raftIdStorage.writeState( raftId );
        return new BoundState( raftId, snapshot );
    }

    private boolean publishBootstrapper( NamedDatabaseId namedDatabaseId, CoreSnapshot snapshot,
            BindingConditions bindingConditions, boolean mayBeManyBootstrappers ) throws Exception
    {
        raftId = RaftId.from( namedDatabaseId.databaseId() );
        var publishSucceeded = false;
        if ( snapshot != null )
        {
            do
            {
                publishSucceeded = publishRaftId( raftId, mayBeManyBootstrappers );
                retryWaiter.apply();
            } while ( !publishSucceeded && bindingConditions.shouldContinuePublishing( namedDatabaseId ) );
            monitor.bootstrapped( snapshot, namedDatabaseId, raftId );
        }

        return publishSucceeded;
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
                case FAILED_PUBLISH:
                    return false;
                case SUCCESSFUL_PUBLISH_BY_OTHER:
                    return ifNotExists;
                case SUCCESSFUL_PUBLISH_BY_ME:
                    return true;
                default:
                    throw new BindingException( format( "Unexpected outcome %s when trying to publish raftId %s", outcome.name(), localRaftId ) );
            }
        }
        catch ( BindingException e )
        {
            throw e;
        }
        catch ( Throwable t )
        {
            throw new BindingException( format( "Failed to publish raftId %s", localRaftId ), t );
        }
    }

    private static class BindingConditions
    {
        private final DatabaseStartAborter startAborter;
        private final Clock clock;
        private final long endTime;

        BindingConditions( DatabaseStartAborter startAborter, Clock clock, Duration timeout )
        {
            this.startAborter = startAborter;
            this.clock = clock;
            this.endTime = clock.millis() + timeout.toMillis();
        }

        boolean shouldContinueBinding( NamedDatabaseId namedDatabaseId, DatabaseCoreTopology topology ) throws TimeoutException, DatabaseStartAbortedException
        {
            var message = format( "Failed to join or bootstrap a raft group with id %s and members %s in time. " +
                    "Please restart the cluster.", RaftId.from( namedDatabaseId.databaseId() ), topology );
            return shouldContinue( namedDatabaseId, message );
        }

        boolean shouldContinuePublishing( NamedDatabaseId namedDatabaseId ) throws TimeoutException, DatabaseStartAbortedException
        {
            var message = format( "Failed to publish raftId %s in time. Please restart the cluster.", RaftId.from( namedDatabaseId.databaseId() ) );
            return shouldContinue( namedDatabaseId, message );
        }

        private boolean shouldContinue( NamedDatabaseId namedDatabaseId, String timeoutMessage ) throws TimeoutException, DatabaseStartAbortedException
        {
            var shouldAbort = startAborter.shouldAbort( namedDatabaseId );
            var timedOut = endTime < clock.millis();

            if ( shouldAbort )
            {
                throw new DatabaseStartAbortedException( namedDatabaseId );
            }
            else if ( timedOut )
            {
                throw new TimeoutException( timeoutMessage );
            }
            return true;
        }
    }

    private static class BootstrapDatabaseLogger
    {
        private final DatabaseLog log;
        private final CappedLogger discoveryServiceAttemptLog;
        private final CappedLogger initialMembersAttempLog;

        private BootstrapDatabaseLogger( DatabaseLog log, Clock clock )
        {
            discoveryServiceAttemptLog = new CappedLogger( log ).setTimeLimit( 10, TimeUnit.SECONDS, clock );
            initialMembersAttempLog = new CappedLogger( log ).setTimeLimit( 10, TimeUnit.SECONDS, clock );
            this.log = log;
        }

        private void logRemoveSystemDatabase()
        {
            log.info( "Removing system database to force store copy" );
        }

        private void logBootstrapAttemptWithDiscoveryService()
        {
            discoveryServiceAttemptLog.info( "Trying bootstrap using discovery service method" );
        }

        private void logBootstrapWithInitialMembersAndStoreID( Set<MemberId> initialMembers, StoreId storeId )
        {
            initialMembersAttempLog.info( "Trying bootstrap using initial members %s and store ID %s", initialMembers, storeId );
        }
    }
}
