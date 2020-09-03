/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.RaftBootstrapper;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.function.ThrowingAction;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.discovery.PublishRaftIdOutcome.MAYBE_PUBLISHED;
import static com.neo4j.causalclustering.identity.RaftBinder.Publisher.ME;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class RaftBinder implements Supplier<Optional<RaftGroupId>>
{
    public interface Monitor
    {
        void waitingForCoreMembers( NamedDatabaseId namedDatabaseId, int minimumCount );

        void waitingForBootstrap( NamedDatabaseId namedDatabaseId );

        void bootstrapped( CoreSnapshot snapshot, NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId );

        void boundToRaftFromDisk( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId );

        void boundToRaftThroughTopology( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId );

        void retryPublishRaftId( NamedDatabaseId namedDatabaseId, RaftGroupId raftGroupId );

        void logSaveSystemDatabase();

        void bootstrapAttemptUsingDiscovery( NamedDatabaseId namedDatabaseId );

        void bootstrapAttempt( Set<ServerId> initialServers, StoreId storeId );
    }

    private final NamedDatabaseId namedDatabaseId;
    private final SimpleStorage<RaftGroupId> raftGroupIdStorage;
    private final CoreTopologyService topologyService;
    private final ClusterSystemGraphDbmsModel systemGraph;
    private final RaftBootstrapper raftBootstrapper;
    private final CoreServerIdentity myIdentity;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final int minCoreHosts;
    private final boolean refuseToBeLeader;

    private RaftGroupId raftGroupId;

    public RaftBinder( NamedDatabaseId namedDatabaseId, CoreServerIdentity myIdentity, SimpleStorage<RaftGroupId> raftGroupIdStorage,
            CoreTopologyService topologyService, ClusterSystemGraphDbmsModel systemGraph, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
            Duration timeout, RaftBootstrapper raftBootstrapper, int minCoreHosts, boolean refuseToBeLeader, Monitors monitors )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.myIdentity = myIdentity;
        this.systemGraph = systemGraph;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.raftGroupIdStorage = raftGroupIdStorage;
        this.topologyService = topologyService;
        this.raftBootstrapper = raftBootstrapper;
        this.clock = clock;
        this.retryWaiter = retryWaiter;
        this.timeout = timeout;
        this.minCoreHosts = minCoreHosts;
        this.refuseToBeLeader = refuseToBeLeader;
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
        var serverCount = coreTopology.servers().size();
        if ( serverCount < minCoreHosts )
        {
            monitor.waitingForCoreMembers( namedDatabaseId, minCoreHosts );
            return false;
        }
        else if ( !topologyService.canBootstrapDatabase( namedDatabaseId ) )
        {
            monitor.waitingForBootstrap( namedDatabaseId );
            return false;
        }
        return true;
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

        if ( raftGroupIdStorage.exists() )
        {
            return bindToExistingRaft( bindingConditions );
        }
        else
        {
            return bindToNewRaft( bindingConditions );
        }
    }

    private BoundState bindToNewRaft( BindingConditions bindingConditions ) throws Exception
    {
        /* The binding protocol towards other servers has to be re-written if we are to support random RaftMemberIDs. */
        var raftMemberId = new RaftMemberId( myIdentity.serverId().uuid() );
        myIdentity.createMemberId( namedDatabaseId, raftMemberId );
        topologyService.onRaftMemberKnown( namedDatabaseId );

        if ( isInitialDatabase() )
        {
            return bindToInitialRaftGroup( bindingConditions, raftMemberId );
        }
        else
        {
            var initialServers = systemGraph.getInitialServers( namedDatabaseId ).stream().map( ServerId::new ).collect( toSet() );
            return bindToRaftGroupNotPartOfInitialDatabases( bindingConditions, initialServers, raftMemberId );
        }
    }

    private BoundState bindToExistingRaft( BindingConditions bindingConditions ) throws Exception
    {
        // If raft id state exists, read it and verify that it corresponds to the database being started
        raftGroupId = raftGroupIdStorage.readState();
        validateRaftId( raftGroupId, namedDatabaseId );

        var memberId = myIdentity.loadMemberId( namedDatabaseId );
        topologyService.onRaftMemberKnown( namedDatabaseId );
        awaitPublishRaftId( bindingConditions, raftGroupId, memberId );

        monitor.boundToRaftFromDisk( namedDatabaseId, myIdentity.raftMemberId( namedDatabaseId ) );
        return new BoundState( raftGroupId );
    }

    private static void validateRaftId( RaftGroupId raftGroupId, NamedDatabaseId namedDatabaseId )
    {
        if ( !Objects.equals( raftGroupId.uuid(), namedDatabaseId.databaseId().uuid() ) )
        {
            throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                            "This may indicate a previous DROP operation for %s did not complete.",
                    raftGroupId.uuid(), namedDatabaseId.databaseId().uuid(), namedDatabaseId ) );
        }
    }

    private BoundState bindToInitialRaftGroup( BindingConditions bindingConditions, RaftMemberId raftMemberId ) throws Exception
    {
        DatabaseCoreTopology topology;
        CoreSnapshot snapshot;

        while ( true )
        {
            topology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            if ( bootstrappedByOther( topology ) )
            {
                validateRaftId( topology.raftGroupId(), namedDatabaseId );
                // Someone else bootstrapped, we're done!
                return handleBootstrapByOther( topology, raftMemberId );
            }
            if ( hostShouldBootstrapRaft( topology ) )
            {
                var raftGroupId = RaftGroupId.from( namedDatabaseId.databaseId() );

                monitor.bootstrapAttemptUsingDiscovery( namedDatabaseId );

                var outcome = publishRaftId( raftGroupId, raftMemberId, bindingConditions );
                if ( Publisher.successfullyPublishedBy( ME, outcome ) )
                {
                    this.raftGroupId = raftGroupId;
                    Set<RaftMemberId> initialMemberIds = topology.servers()
                                                                 .keySet()
                                                                 .stream()
                                                                 .map( serverId -> new RaftMemberId( serverId.uuid() ) )
                                                                 .collect( toSet() );

                    snapshot = raftBootstrapper.bootstrap( initialMemberIds );
                    monitor.bootstrapped( snapshot, namedDatabaseId, myIdentity.raftMemberId( namedDatabaseId ) );
                    return new BoundState( raftGroupId, snapshot );
                }
            }
            bindingConditions.allowContinueBindingWaitForCores( namedDatabaseId, topology );
            retryWaiter.apply();
        }
    }

    private BoundState awaitBootstrapByOther( BindingConditions bindingConditions, RaftMemberId raftMemberId ) throws Exception
    {
        DatabaseCoreTopology topology;
        while ( true )
        {
            topology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            if ( bootstrappedByOther( topology ) )
            {
                validateRaftId( topology.raftGroupId(), namedDatabaseId );
                // Someone else bootstrapped, we're done!
                return handleBootstrapByOther( topology, raftMemberId );
            }
            bindingConditions.allowContinueBindingWaitForOthers( namedDatabaseId, topology );
        }
    }

    private BoundState bindToRaftGroupNotPartOfInitialDatabases( BindingConditions bindingConditions, Set<ServerId> initialServers, RaftMemberId raftMemberId )
            throws Exception
    {
        if ( !initialServers.contains( myIdentity.serverId() ) || refuseToBeLeader )
        {
            return awaitBootstrapByOther( bindingConditions, raftMemberId );
        }
        else
        {
            // Used for databases created during runtime in response to operator commands.
            StoreId storeId = systemGraph.getStoreId( namedDatabaseId );
            var raftGroupId = RaftGroupId.from( namedDatabaseId.databaseId() );
            monitor.bootstrapAttempt( initialServers, storeId );
            awaitPublishRaftId( bindingConditions, raftGroupId, raftMemberId );
            this.raftGroupId = raftGroupId;

            var initialMemberIds = initialServers.stream().map( serverId -> new RaftMemberId( serverId.uuid() ) ).collect( toSet() );
            var snapshot = raftBootstrapper.bootstrap( initialMemberIds, storeId );
            monitor.bootstrapped( snapshot, namedDatabaseId, myIdentity.raftMemberId( namedDatabaseId ) );
            return new BoundState( raftGroupId, snapshot );
        }
    }

    private boolean isInitialDatabase()
    {
        return namedDatabaseId.isSystemDatabase() || systemGraph.getInitialServers( namedDatabaseId ).isEmpty();
    }

    private boolean bootstrappedByOther( DatabaseCoreTopology topology )
    {
        return topology.raftGroupId() != null && !topologyService.didBootstrapDatabase( namedDatabaseId );
    }

    private BoundState handleBootstrapByOther( DatabaseCoreTopology topology, RaftMemberId raftMemberId ) throws IOException
    {
        saveSystemDatabase();
        monitor.boundToRaftThroughTopology( namedDatabaseId, raftMemberId );
        raftGroupId = topology.raftGroupId();
        return new BoundState( raftGroupId );
    }

    /**
     * Because the bootstrapping instance might modify the seed and change
     * the store ID, other instances have to remove their seeds and perform
     * a store copy later in the startup.
     */
    private void saveSystemDatabase() throws IOException
    {
        if ( namedDatabaseId.isSystemDatabase() )
        {
            monitor.logSaveSystemDatabase();
            raftBootstrapper.saveStore();
        }
    }

    @Override
    public Optional<RaftGroupId> get()
    {
        return Optional.ofNullable( raftGroupId );
    }

    private void awaitPublishRaftId( BindingConditions bindingConditions, RaftGroupId raftGroupId, RaftMemberId memberId ) throws Exception
    {
        while ( true )
        {
            var outcome = publishRaftId( raftGroupId, memberId, bindingConditions );
            if ( outcome != MAYBE_PUBLISHED )
            {
                return;
            }
            monitor.retryPublishRaftId( namedDatabaseId, raftGroupId );
            retryWaiter.apply();
            bindingConditions.allowContinuePublishing( namedDatabaseId );
        }
    }

    /**
     * Publish the raft group ID for the database being started to the discovery service, as a signal to other members that this database has been bootstrapped.
     *
     * @param localRaftGroupId the raft group ID to be published
     * @param bindingConditions determines if the publish step is allowed to retry on {@link TimeoutException}
     * @return the outcome of the publish attempt (i.e. whether successfully published by us, other or not at all)
     */
    private PublishRaftIdOutcome publishRaftId( RaftGroupId localRaftGroupId, RaftMemberId memberId, BindingConditions bindingConditions ) throws Exception
    {
        PublishRaftIdOutcome outcome;
        do
        {
            try
            {
                outcome = topologyService.publishRaftId( localRaftGroupId, memberId );
            }
            catch ( TimeoutException e )
            {
                outcome = null;
            }
            catch ( Throwable t )
            {
                throw new BindingException( format( "Failed to publish raftId %s", localRaftGroupId ), t );
            }
        }
        while ( outcome == null && bindingConditions.allowContinuePublishing( namedDatabaseId ) );
        return outcome;
    }

    enum Publisher
    {
        ME
                {
                    @Override
                    boolean validate( PublishRaftIdOutcome outcome )
                    {
                        return outcome == PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME;
                    }
                },
        ANYONE
                {
                    @Override
                    boolean validate( PublishRaftIdOutcome outcome )
                    {
                        return outcome == PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME || outcome == PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER;
                    }
                };

        abstract boolean validate( PublishRaftIdOutcome outcome );

        public static boolean successfullyPublishedBy( Publisher publisher, PublishRaftIdOutcome outcome )
        {
            return publisher.validate( outcome );
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

        void allowContinueBindingWaitForCores( NamedDatabaseId namedDatabaseId, DatabaseCoreTopology topology )
                throws TimeoutException, DatabaseStartAbortedException
        {
            Supplier<String> message = () -> format( "Failed to join or bootstrap a raft group with id %s and members %s in time. " +
                                                     "Please restart the cluster. Clue: not enough cores found",
                                                     RaftGroupId.from( namedDatabaseId.databaseId() ), topology );
            allowContinue( namedDatabaseId, message );
        }

        void allowContinueBindingWaitForOthers( NamedDatabaseId namedDatabaseId, DatabaseCoreTopology topology )
                throws TimeoutException, DatabaseStartAbortedException
        {
            Supplier<String> message = () -> format( "Failed to join or bootstrap a raft group with id %s and members %s in time. " +
                                                     "Please restart the cluster. Clue: member is not part of initial set, other have not bootstrapped",
                                                     RaftGroupId.from( namedDatabaseId.databaseId() ), topology );
            allowContinue( namedDatabaseId, message );
        }

        boolean allowContinuePublishing( NamedDatabaseId namedDatabaseId )
                throws TimeoutException, DatabaseStartAbortedException
        {
            Supplier<String> message = () -> format( "Failed to publish raftId %s in time. Please restart the cluster.",
                                                     RaftGroupId.from( namedDatabaseId.databaseId() ) );
            return allowContinue( namedDatabaseId, message );
        }

        private boolean allowContinue( NamedDatabaseId namedDatabaseId, Supplier<String> timeoutMessage ) throws TimeoutException, DatabaseStartAbortedException
        {
            var shouldAbort = startAborter.shouldAbort( namedDatabaseId );
            var timedOut = endTime < clock.millis();

            if ( shouldAbort )
            {
                throw new DatabaseStartAbortedException( namedDatabaseId );
            }
            else if ( timedOut )
            {
                throw new TimeoutException( timeoutMessage.get() );
            }
            return true;
        }
    }
}
