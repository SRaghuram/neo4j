/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyListenerService;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftIdSetRequest;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseStateActor;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.CoreDiscoveryMemberFactory;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;
import static java.lang.String.format;

public class AkkaCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    private final RetryStrategy catchupAddressRetryStrategy;
    private final Restarter restarter;
    private final CoreDiscoveryMemberFactory memberSnapshotFactory;
    private final JobScheduler jobScheduler;
    private final Executor executor;
    private final Clock clock;
    private final ReplicatedDataMonitor replicatedDataMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private final DatabaseStateService databaseStateService;
    private final Config config;
    private final CoreServerIdentity myIdentity;
    private final Log log;
    private final Log userLog;

    private final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    private final Map<NamedDatabaseId,LeaderInfo> localLeadersByDatabaseId = new ConcurrentHashMap<>();

    private volatile boolean isRestarting;
    private volatile ActorRef coreTopologyActorRef;
    private volatile ActorRef directoryActorRef;
    private volatile ActorRef databaseStateActorRef;
    private volatile GlobalTopologyState globalTopologyState;

    public AkkaCoreTopologyService( Config config, CoreServerIdentity myIdentity, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, Restarter restarter,
            CoreDiscoveryMemberFactory memberSnapshotFactory, JobScheduler jobScheduler, Clock clock, Monitors monitors,
            DatabaseStateService databaseStateService )
    {
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restarter = restarter;
        this.memberSnapshotFactory = memberSnapshotFactory;
        this.jobScheduler = jobScheduler;
        this.executor = jobScheduler.executor( Group.AKKA_HELPER );
        this.clock = clock;
        this.config = config;
        this.myIdentity = myIdentity;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.replicatedDataMonitor = monitors.newMonitor( ReplicatedDataMonitor.class );
        this.globalTopologyState = newGlobalTopologyState( logProvider, listenerService );
        this.clusterSizeMonitor = monitors.newMonitor( ClusterSizeMonitor.class );
        this.databaseStateService = databaseStateService;
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClusterActorSystem(() -> executor.execute(this::restart));
        var coreTopologySink = actorSystemLifecycle.queueMostRecent( this::onCoreTopologyMessage );
        var directorySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        var bootstrapStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onBootstrapStateUpdate );
        var databaseStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbStateUpdate );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        var raftMappingSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onRaftMappingUpdate );

        var cluster = actorSystemLifecycle.cluster();
        var replicator = actorSystemLifecycle.replicator();
        var rrTopologyActor = readReplicaTopologyActor( rrTopologySink, databaseStateSink );
        coreTopologyActorRef = coreTopologyActor( cluster, replicator, coreTopologySink, bootstrapStateSink, raftMappingSink, rrTopologyActor );
        directoryActorRef = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        databaseStateActorRef = databaseStateActor( cluster, replicator, databaseStateSink, rrTopologyActor );
        publishInitialData( coreTopologyActorRef, directoryActorRef, databaseStateActorRef );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink, SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink,
            ActorRef rrTopologyActor )
    {
        var topologyBuilder = new TopologyBuilder();
        var coreTopologyProps = CoreTopologyActor.props(
                topologySink,
                bootstrapStateSink,
                raftMappingSink,
                rrTopologyActor,
                replicator,
                cluster,
                topologyBuilder,
                config,
                myIdentity,
                replicatedDataMonitor,
                clusterSizeMonitor );
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        var directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor, replicatedDataMonitor );
        return actorSystemLifecycle.applicationActorOf( directoryProps, DirectoryActor.NAME );
    }

    private Map<DatabaseId,LeaderInfo> localLeadershipsSnapshot()
    {
        return localLeadersByDatabaseId.entrySet().stream()
                                       .collect( Collectors.toUnmodifiableMap( entry -> entry.getKey().databaseId(), Map.Entry::getValue ) );
    }

    private ActorRef databaseStateActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> stateSink,
            ActorRef rrTopologyActor )
    {
        Props stateProps = DatabaseStateActor.props( cluster, replicator, stateSink, rrTopologyActor, replicatedDataMonitor, myIdentity.serverId() );
        return actorSystemLifecycle.applicationActorOf( stateProps, DatabaseStateActor.NAME );
    }

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
                                               SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, databaseStateSink, receptionist, config, clock );
        return actorSystemLifecycle.applicationActorOf( readReplicaTopologyProps, ReadReplicaTopologyActor.NAME );
    }

    private void publishInitialData( ActorRef... actorRefs )
    {
        var memberSnapshot = memberSnapshotFactory.createSnapshot( myIdentity, databaseStateService, localLeadershipsSnapshot() );
        for ( ActorRef actorRef : actorRefs )
        {
            actorRef.tell( new PublishInitialData( memberSnapshot ), noSender() );
        }
    }

    private void onCoreTopologyMessage( CoreTopologyMessage coreTopologyMessage )
    {
        globalTopologyState.onTopologyUpdate( coreTopologyMessage.coreTopology() );
        actorSystemLifecycle.addSeenAddresses( coreTopologyMessage.akkaMembers() );
    }

    @Override
    public synchronized void stop0() throws Exception
    {
        coreTopologyActorRef = null;
        directoryActorRef = null;
        databaseStateActorRef = null;

        actorSystemLifecycle.shutdown();

        if ( this.isRestarting )
        {
            globalTopologyState.clearRemoteData( serverId() );
        }
        else
        {
            // set globalTopologyState to a new empty state so that we don't report any remote member info but we avoid NPEs
            globalTopologyState = newGlobalTopologyState( logProvider, listenerService );
        }

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
        listener.onCoreTopologyChange( coreTopologyForDatabase( listener.namedDatabaseId() ).members( globalTopologyState::resolveRaftMemberForServer ) );
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

    @Override
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return globalTopologyState.resolveRaftMemberForServer( namedDatabaseId.databaseId(), serverId );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId raftMemberId )
    {
        return globalTopologyState.resolveServerForRaftMember( raftMemberId );
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

    @VisibleForTesting
    public synchronized void restart()
    {
        if ( !SafeLifecycle.State.RUN.equals( state() ) )
        {
            log.info( "Not restarting because not running. State is %s", state() );
            return;
        }

        userLog.info( "Restarting discovery system after probable network partition" );

        restarter.restart( this::doRestart );
    }

    private boolean doRestart()
    {
        try
        {
            isRestarting = true;
            stop();
            start();
            userLog.info( "Successfully restarted discovery system" );
            return true;
        }
        catch ( Throwable t )
        {
            userLog.warn( "Failed to restart discovery system", t );
            return false;
        }
        finally
        {
            isRestarting = false;
        }
    }

    @Override
    public void onDatabaseStart( NamedDatabaseId namedDatabaseId )
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new DatabaseStartedMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void onRaftMemberKnown( NamedDatabaseId namedDatabaseId )
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new RaftMemberKnownMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
        var removedInfo = localLeadersByDatabaseId.remove( namedDatabaseId );
        if ( removedInfo != null )
        {
            log.info( "I am member %s. Removed leader info of member %s %s and term %s", memberId(), removedInfo.memberId(), namedDatabaseId,
                      removedInfo.term() );
        }
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new DatabaseStoppedMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
        var stateActor = databaseStateActorRef;
        if ( stateActor != null )
        {
            stateActor.tell( DiscoveryDatabaseState.from( newState ), noSender() );
        }
    }

    @Override
    public void setLeader( LeaderInfo newLeaderInfo, NamedDatabaseId namedDatabaseId )
    {
        var currentLeaderInfo = getLocalLeader( namedDatabaseId );
        if ( currentLeaderInfo.term() < newLeaderInfo.term() )
        {
            log.info( "I am server %s. Updating leader info to member %s %s and term %s", serverId(), newLeaderInfo.memberId(), namedDatabaseId,
                    newLeaderInfo.term() );
            localLeadersByDatabaseId.put( namedDatabaseId, newLeaderInfo );
            sendLeaderInfo( newLeaderInfo, namedDatabaseId );
        }
    }

    @Override
    public void handleStepDown( long term, NamedDatabaseId namedDatabaseId )
    {
        var currentLeaderInfo = getLocalLeader( namedDatabaseId );

        var wasLeaderForTerm =
                Objects.equals( myIdentity.raftMemberId( namedDatabaseId ), currentLeaderInfo.memberId() ) &&
                term == currentLeaderInfo.term();

        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader for %s in term %s, now moving " +
                      "to follower.", serverId(), namedDatabaseId, currentLeaderInfo.term() );

            var newLeaderInfo = currentLeaderInfo.stepDown();
            localLeadersByDatabaseId.put( namedDatabaseId, newLeaderInfo );
            sendLeaderInfo( newLeaderInfo, namedDatabaseId );
        }
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

    @Override
    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.getLeader( namedDatabaseId );
    }

    @Override
    public Map<ServerId,CoreServerInfo> allCoreServers()
    {
        return globalTopologyState.allCoreServers();
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.coreTopologyForDatabase( namedDatabaseId );
    }

    @Override
    public Map<ServerId,ReadReplicaInfo> allReadReplicas()
    {
        return globalTopologyState.allReadReplicas();
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.readReplicaTopologyForDatabase( namedDatabaseId );
    }

    @Override
    public SocketAddress lookupCatchupAddress( ServerId upstream ) throws CatchupAddressResolutionException
    {
        try
        {
            return catchupAddressRetryStrategy.apply( () -> globalTopologyState.retrieveCatchupServerAddress( upstream ), Objects::nonNull );
        }
        catch ( TimeoutException e )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
    }

    @Override
    public ServerId serverId()
    {
        return myIdentity.serverId();
    }

    @Override
    public DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return globalTopologyState.stateFor( serverId, namedDatabaseId );
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allCoreStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.copyOf( globalTopologyState.coreStatesForDatabase( namedDatabaseId ).memberStates() );
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.copyOf( globalTopologyState.readReplicaStatesForDatabase( namedDatabaseId ).memberStates() );
    }

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }

    private void sendLeaderInfo( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId )
    {
        var directoryActor = directoryActorRef;
        if ( directoryActor != null )
        {
            directoryActor.tell( new LeaderInfoSettingMessage( leaderInfo, namedDatabaseId ), noSender() );
        }
    }

    private GlobalTopologyState newGlobalTopologyState( LogProvider logProvider, CoreTopologyListenerService listenerService )
    {
        return new GlobalTopologyState( logProvider, listenerService::notifyListeners, jobScheduler );
    }

    private LeaderInfo getLocalLeader( NamedDatabaseId namedDatabaseId )
    {
        return localLeadersByDatabaseId.getOrDefault( namedDatabaseId, LeaderInfo.INITIAL );
    }

    @Override
    public boolean isHealthy()
    {
        return restarter.isHealthy();
    }

    @VisibleForTesting
    public Cluster getAkkaCluster()
    {
        return actorSystemLifecycle.cluster();
    }
}
