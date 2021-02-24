/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftIdSetRequest;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static akka.actor.ActorRef.noSender;
import static java.lang.String.format;

public class AkkaCoreTopologyService extends AkkaTopologyService implements CoreTopologyService
{
    private final Map<NamedDatabaseId,LeaderInfo> localLeadersByDatabaseId = new ConcurrentHashMap<>();
    private final CoreServerIdentity myIdentity;
    public AkkaCoreTopologyService( Config config,
                                    CoreServerIdentity myIdentity,
                                    ActorSystemLifecycle actorSystemLifecycle,
                                    LogProvider logProvider,
                                    LogProvider userLogProvider,
                                    RetryStrategy catchupAddressRetryStrategy,
                                    ActorSystemRestarter actorSystemRestarter,
                                    ServerSnapshotFactory serverSnapshotFactory,
                                    JobScheduler jobScheduler,
                                    Clock clock,
                                    Monitors monitors,
                                    DatabaseStateService databaseStateService,
                                    Panicker panicker )
    {
        super( config,
               myIdentity,
               actorSystemLifecycle,
               logProvider,
               userLogProvider,
               catchupAddressRetryStrategy,
               actorSystemRestarter,
               serverSnapshotFactory,
               jobScheduler,
               clock,
               monitors,
               databaseStateService,
               panicker );
        this.myIdentity = myIdentity;
    }

    @Override
    protected ServerSnapshot createServerSnapshot()
    {
        return serverSnapshotFactory.createSnapshot( databaseStateService, localLeadershipsSnapshot() );
    }

    private Map<DatabaseId,LeaderInfo> localLeadershipsSnapshot()
    {
        return localLeadersByDatabaseId.entrySet().stream()
                                       .collect( Collectors.toUnmodifiableMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    @Override
    public SocketAddress lookupRaftAddress( RaftMemberId target )
    {
        return globalTopologyState.getCoreServerInfo( target ).map( CoreServerInfo::getRaftServer ).orElse( null );
    }

    @Override
    public void addLocalCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreTopologyForDatabase( listener.namedDatabaseId() ).resolve( globalTopologyState::resolveRaftMemberForServer ) );
    }

    @Override
    public void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public PublishRaftIdOutcome publishRaftId( RaftGroupId raftGroupId, RaftMemberId memberId ) throws TimeoutException
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            var timeout = config.get( CausalClusteringInternalSettings.raft_id_publish_timeout );
            var request = new RaftIdSetRequest( raftGroupId, memberId, timeout );
            var idSetJob = Patterns.ask( coreTopologyActor, request, timeout )
                                   .thenApplyAsync( this::checkOutcome, executor )
                                   .toCompletableFuture();
            try
            {
                // Although the idSetJob has a timeout it needs the actor system to enforce it.
                // We have observed that the Akka system can hang and then the timeout never throws.
                // So we timeout fetching this future because enforcing this timeout doesn't depend on Akka.
                return idSetJob.get( timeout.toNanos(), TimeUnit.NANOSECONDS );
            }
            catch ( CompletionException | InterruptedException | ExecutionException e )
            {
                if ( e.getCause() instanceof AskTimeoutException )
                {
                    throw new TimeoutException( "Could not publish raft id within " + timeout.toSeconds() + " seconds" );
                }
                throw new RuntimeException( e.getCause() );
            }
        }
        return PublishRaftIdOutcome.MAYBE_PUBLISHED;
    }

    private PublishRaftIdOutcome checkOutcome( Object response )
    {
        if ( !(response instanceof PublishRaftIdOutcome) )
        {
            throw new IllegalArgumentException( format( "Unexpected response when attempting to publish raftId. Expected %s, received %s",
                                                        PublishRaftIdOutcome.class.getSimpleName(), response.getClass().getCanonicalName() ) );
        }
        return (PublishRaftIdOutcome) response;
    }

    @Override
    public boolean canBootstrapDatabase( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.bootstrapState().canBootstrapRaft( namedDatabaseId );
    }

    @Override
    public boolean didBootstrapDatabase( NamedDatabaseId namedDatabaseId )
    {
        var thisRaftMember = resolveRaftMemberForServer( namedDatabaseId, myIdentity.serverId() );
        return globalTopologyState.bootstrapState().memberBootstrappedRaft( namedDatabaseId, thisRaftMember );
    }

    @Override
    public void onRaftMemberKnown( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new RaftMemberKnownMessage( namedDatabaseId, raftMemberId ), noSender() );
        }
    }

    @Override
    protected void removeLeaderInfo( NamedDatabaseId namedDatabaseId )
    {
        var removedInfo = localLeadersByDatabaseId.remove( namedDatabaseId );
        if ( removedInfo != null )
        {
            log.info( "I am server %s. Removed leader info of member %s %s and term %s", serverId(), removedInfo.memberId(), namedDatabaseId,
                      removedInfo.term() );
        }
    }

    @Override
    public void setLeader( LeaderInfo newLeaderInfo, NamedDatabaseId namedDatabaseId )
    {
        localLeadersByDatabaseId.compute( namedDatabaseId, ( databaseId, leaderInfo ) ->
        {
            var currentLeaderInfo = leaderInfo != null ? leaderInfo : LeaderInfo.INITIAL;
            if ( currentLeaderInfo.term() < newLeaderInfo.term() )
            {
                log.info( "I am server %s. Updating leader info to member %s %s and term %s", serverId(), newLeaderInfo.memberId(), namedDatabaseId,
                          newLeaderInfo.term() );
                sendLeaderInfo( newLeaderInfo, namedDatabaseId );
                return newLeaderInfo;
            }
            return currentLeaderInfo;
        } );
    }

    @Override
    public void handleStepDown( long term, NamedDatabaseId namedDatabaseId )
    {
        localLeadersByDatabaseId.computeIfPresent( namedDatabaseId, ( databaseId, currentLeaderInfo ) ->
        {
            var wasLeaderForTerm =
                    Objects.equals( myIdentity.raftMemberId( namedDatabaseId ), currentLeaderInfo.memberId() ) &&
                    term == currentLeaderInfo.term();

            if ( wasLeaderForTerm )
            {
                log.info( "Step down event detected. This topology member, with MemberId %s, was leader for %s in term %s, now moving " +
                          "to follower.", serverId(), namedDatabaseId, currentLeaderInfo.term() );

                var newLeaderInfo = currentLeaderInfo.stepDown();
                sendLeaderInfo( newLeaderInfo, namedDatabaseId );
                return newLeaderInfo;
            }
            return currentLeaderInfo;
        } );
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        var leaderInfo = localLeadersByDatabaseId.get( namedDatabaseId );
        var raftMemberId = globalTopologyState.resolveRaftMemberForServer( namedDatabaseId.databaseId(), serverId );
        if ( leaderInfo != null && Objects.equals( raftMemberId, leaderInfo.memberId() ) )
        {
            return RoleInfo.LEADER;
        }
        return globalTopologyState.role( namedDatabaseId, serverId );
    }

    private void sendLeaderInfo( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId )
    {
        var directoryActor = directoryActorRef;
        if ( directoryActor != null )
        {
            directoryActor.tell( new LeaderInfoSettingMessage( leaderInfo, namedDatabaseId ), noSender() );
        }
    }
}
