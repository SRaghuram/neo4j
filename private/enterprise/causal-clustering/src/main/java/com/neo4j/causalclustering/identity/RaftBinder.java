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
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.discovery.PublishRaftIdOutcome.FAILED_PUBLISH;
import static com.neo4j.causalclustering.identity.RaftBinder.Publisher.ME;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class RaftBinder implements Supplier<Optional<RaftId>>
{
    public interface Monitor
    {
        void waitingForCoreMembers( NamedDatabaseId namedDatabaseId, int minimumCount );

        void waitingForBootstrap( NamedDatabaseId namedDatabaseId );

        void bootstrapped( CoreSnapshot snapshot, NamedDatabaseId namedDatabaseId, RaftId raftId, MemberId myself );

        void boundToRaftFromDisk( NamedDatabaseId namedDatabaseId, RaftId raftId, MemberId myself );

        void boundToRaftThroughTopology( NamedDatabaseId namedDatabaseId, RaftId raftId, MemberId myself );

        void retryPublishRaftId( NamedDatabaseId namedDatabaseId, RaftId raftId );

        void logSaveSystemDatabase();

        void logBootstrapAttemptWithDiscoveryService();

        void logBootstrapWithInitialMembersAndStoreID( Set<MemberId> initialMembers, StoreId storeId );
    }

    private final NamedDatabaseId namedDatabaseId;
    private final SimpleStorage<RaftId> raftIdStorage;
    private final CoreTopologyService topologyService;
    private final ClusterSystemGraphDbmsModel systemGraph;
    private final RaftBootstrapper raftBootstrapper;
    private final RaftMemberId meAsRaftMember;
    private final MemberId meAsServer;
    private final Monitor monitor;
    private final Clock clock;
    private final ThrowingAction<InterruptedException> retryWaiter;
    private final Duration timeout;
    private final int minCoreHosts;
    private final boolean refuseToBeLeader;

    private RaftId raftId;

    public RaftBinder( NamedDatabaseId namedDatabaseId, RaftMemberId meAsRaftMember, MemberId meAsServer, SimpleStorage<RaftId> raftIdStorage,
            CoreTopologyService topologyService, ClusterSystemGraphDbmsModel systemGraph, Clock clock, ThrowingAction<InterruptedException> retryWaiter,
            Duration timeout, RaftBootstrapper raftBootstrapper, int minCoreHosts, boolean refuseToBeLeader, Monitors monitors )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.meAsRaftMember = meAsRaftMember;
        this.meAsServer = meAsServer;
        this.systemGraph = systemGraph;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.raftIdStorage = raftIdStorage;
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
        int memberCount = coreTopology.servers().size();
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
            return getBoundState( bindingConditions );
        }
    }

    private BoundState getBoundState( BindingConditions bindingConditions ) throws Exception
    {
        if ( isInitialDatabase() )
        {
            return bindToInitialRaftGroup( bindingConditions );
        }
        else
        {
            var initialMemberIds = systemGraph.getInitialMembers( namedDatabaseId ).stream().map( MemberId::of ).collect( toSet() );
            return bindToRaftGroupNotPartOfInitialDatabases( bindingConditions, initialMemberIds );
        }
    }

    private BoundState bindToRaftIdFromDisk( BindingConditions bindingConditions ) throws Exception
    {
        // If raft id state exists, read it and verify that it corresponds to the database being started
        raftId = raftIdStorage.readState();
        validateRaftId( raftId, namedDatabaseId );

        awaitPublishRaftId( namedDatabaseId, bindingConditions, raftId );

        monitor.boundToRaftFromDisk( namedDatabaseId, raftId, topologyService.memberId() );
        return new BoundState( raftId );
    }

    private static void validateRaftId( RaftId raftId, NamedDatabaseId namedDatabaseId )
    {
        if ( !Objects.equals( raftId.uuid(), namedDatabaseId.databaseId().uuid() ) )
        {
            throw new IllegalStateException( format( "Pre-existing cluster state found with an unexpected id %s. The id for this database is %s. " +
                            "This may indicate a previous DROP operation for %s did not complete.",
                    raftId.uuid(), namedDatabaseId.databaseId().uuid(), namedDatabaseId.name() ) );
        }
    }

    private BoundState bindToInitialRaftGroup( BindingConditions bindingConditions ) throws Exception
    {
        DatabaseCoreTopology topology;
        CoreSnapshot snapshot;

        while ( true )
        {
            topology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            if ( isAlreadyBootstrapped( topology ) )
            {
                validateRaftId( topology.raftId(), namedDatabaseId );
                // Someone else bootstrapped, we're done!
                return handleBootstrapByOther( topology );
            }
            if ( hostShouldBootstrapRaft( topology ) )
            {
                var raftId = RaftId.from( namedDatabaseId.databaseId() );
                monitor.logBootstrapAttemptWithDiscoveryService();
                var outcome = publishRaftId( raftId, bindingConditions );

                if ( Publisher.successfullyPublishedBy( ME, outcome ) )
                {
                    this.raftId = raftId;
                    var initialMembersIds = topology.servers().keySet();
                    var raftMemberIds = convertToRaftMemberIds( initialMembersIds );
                    snapshot = raftBootstrapper.bootstrap( raftMemberIds );
                    monitor.bootstrapped( snapshot, namedDatabaseId, raftId, topologyService.memberId() );
                    return new BoundState( raftId, snapshot );
                }
            }
            bindingConditions.allowContinueBinding( namedDatabaseId, topology );
            retryWaiter.apply();
        }
    }

    private BoundState awaitBootstrapByOther( BindingConditions bindingConditions ) throws IOException, TimeoutException, DatabaseStartAbortedException
    {
        DatabaseCoreTopology topology;
        while ( true )
        {
            topology = topologyService.coreTopologyForDatabase( namedDatabaseId );
            if ( isAlreadyBootstrapped( topology ) )
            {
                validateRaftId( topology.raftId(), namedDatabaseId );
                // Someone else bootstrapped, we're done!
                return handleBootstrapByOther( topology );
            }
            bindingConditions.allowContinueBinding( namedDatabaseId, topology );
        }
    }

    private BoundState bindToRaftGroupNotPartOfInitialDatabases( BindingConditions bindingConditions, Set<MemberId> initialMemberIds ) throws Exception
    {
        if ( !initialMemberIds.contains( meAsServer ) || refuseToBeLeader )
        {
            return awaitBootstrapByOther( bindingConditions );
        }
        else
        {
            // Used for databases created during runtime in response to operator commands.
            StoreId storeId = systemGraph.getStoreId( namedDatabaseId );
            var raftId = RaftId.from( namedDatabaseId.databaseId() );
            monitor.logBootstrapWithInitialMembersAndStoreID( initialMemberIds, storeId );
            var outcome = awaitPublishRaftId( namedDatabaseId, bindingConditions, raftId );
            this.raftId = raftId;

            if ( Publisher.successfullyPublishedBy( ME, outcome ) )
            {
                var raftMemberIds = convertToRaftMemberIds( initialMemberIds );
                var snapshot = raftBootstrapper.bootstrap( raftMemberIds, storeId );
                monitor.bootstrapped( snapshot, namedDatabaseId, raftId, topologyService.memberId() );
                return new BoundState( raftId, snapshot );
            }

            return new BoundState( raftId );
        }
    }

    private Set<RaftMemberId> convertToRaftMemberIds( Set<MemberId> serverIds )
    {
        return serverIds.stream()
                .map( serverId -> topologyService.resolveRaftMemberForServer( namedDatabaseId, serverId ) )
                .collect( Collectors.toSet() );
    }

    private boolean isInitialDatabase()
    {
        return namedDatabaseId.isSystemDatabase() || systemGraph.getInitialMembers( namedDatabaseId ).isEmpty();
    }

    private boolean isAlreadyBootstrapped( DatabaseCoreTopology topology )
    {
        return topology.raftId() != null;
    }

    private BoundState handleBootstrapByOther( DatabaseCoreTopology topology ) throws IOException
    {
        saveSystemDatabase();
        monitor.boundToRaftThroughTopology( namedDatabaseId, raftId, topologyService.memberId() );
        raftId = topology.raftId();
        return new BoundState( raftId );
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
    public Optional<RaftId> get()
    {
        return Optional.ofNullable( raftId );
    }

    private PublishRaftIdOutcome awaitPublishRaftId( NamedDatabaseId namedDatabaseId, BindingConditions bindingConditions, RaftId raftId )
            throws BindingException, InterruptedException, TimeoutException, DatabaseStartAbortedException
    {
        while ( true )
        {
            var outcome = publishRaftId( raftId, bindingConditions );
            if ( outcome != FAILED_PUBLISH )
            {
                return outcome;
            }
            monitor.retryPublishRaftId( namedDatabaseId, raftId );
            retryWaiter.apply();
            bindingConditions.allowContinuePublishing( namedDatabaseId );
        }
    }

    /**
     * Publish the raft Id for the database being started to the discovery service, as a signal to other members that this database has been bootstrapped.
     *
     * @param localRaftId the raftId to be published
     * @param bindingConditions determines if the publish step is allowed to retry on {@link TimeoutException}
     * @return the outcome of the publish attempt (i.e. whether successfully published by us, other or not at all)
     * @throws BindingException in the event of a non-retryable error.
     */
    private PublishRaftIdOutcome publishRaftId( RaftId localRaftId, BindingConditions bindingConditions )
            throws BindingException, TimeoutException, DatabaseStartAbortedException
    {
        PublishRaftIdOutcome outcome;
        do
        {
            try
            {
                outcome = topologyService.publishRaftId( localRaftId, meAsRaftMember );
            }
            catch ( TimeoutException e )
            {
                outcome = null;
            }
            catch ( Throwable t )
            {
                throw new BindingException( format( "Failed to publish raftId %s", localRaftId ), t );
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

        boolean allowContinueBinding( NamedDatabaseId namedDatabaseId, DatabaseCoreTopology topology ) throws TimeoutException, DatabaseStartAbortedException
        {
            var message = format( "Failed to join or bootstrap a raft group with id %s and members %s in time. " +
                                  "Please restart the cluster.", RaftId.from( namedDatabaseId.databaseId() ), topology );
            return allowContinue( namedDatabaseId, message );
        }

        boolean allowContinuePublishing( NamedDatabaseId namedDatabaseId ) throws TimeoutException, DatabaseStartAbortedException
        {
            var message = format( "Failed to publish raftId %s in time. Please restart the cluster.", RaftId.from( namedDatabaseId.databaseId() ) );
            return allowContinue( namedDatabaseId, message );
        }

        private boolean allowContinue( NamedDatabaseId namedDatabaseId, String timeoutMessage ) throws TimeoutException, DatabaseStartAbortedException
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
}
