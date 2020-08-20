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
import akka.event.EventStream;
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
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftIdSetRequest;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseStateActor;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;
import static java.lang.String.format;

public class AkkaCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    private final RetryStrategy catchupAddressRetryStrategy;
    private final Restarter restarter;
    private final DiscoveryMemberFactory discoveryMemberFactory;
    private final Executor executor;
    private final Clock clock;
    private final ReplicatedDataMonitor replicatedDataMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private final Config config;
    private final ClusteringIdentityModule identityModule;
    private final Log log;
    private final Log userLog;

    private final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    private final Map<NamedDatabaseId,LeaderInfo> localLeadersByDatabaseId = new ConcurrentHashMap<>();

    private volatile ActorRef coreTopologyActorRef;
    private volatile ActorRef directoryActorRef;
    private volatile ActorRef databaseStateActorRef;
    private volatile GlobalTopologyState globalTopologyState;

    public AkkaCoreTopologyService( Config config, ClusteringIdentityModule identityModule, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, Restarter restarter,
            DiscoveryMemberFactory discoveryMemberFactory, Executor executor, Clock clock, Monitors monitors )
    {
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restarter = restarter;
        this.discoveryMemberFactory = discoveryMemberFactory;
        this.executor = executor;
        this.clock = clock;
        this.config = config;
        this.identityModule = identityModule;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
        this.replicatedDataMonitor = monitors.newMonitor( ReplicatedDataMonitor.class );
        this.globalTopologyState = newGlobalTopologyState( logProvider, listenerService );
        this.clusterSizeMonitor = monitors.newMonitor( ClusterSizeMonitor.class );
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClusterActorSystem();
        SourceQueueWithComplete<CoreTopologyMessage> coreTopologySink =
                actorSystemLifecycle.queueMostRecent( this::onCoreTopologyMessage );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        SourceQueueWithComplete<BootstrapState> bootstrapStateSink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onBootstrapStateUpdate );
        SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbStateUpdate );

        Cluster cluster = actorSystemLifecycle.cluster();
        ActorRef replicator = actorSystemLifecycle.replicator();
        ActorRef rrTopologyActor = readReplicaTopologyActor( rrTopologySink, databaseStateSink );
        coreTopologyActorRef = coreTopologyActor( cluster, replicator, coreTopologySink, bootstrapStateSink, rrTopologyActor );
        directoryActorRef = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        databaseStateActorRef = databaseStateActor( cluster, replicator, databaseStateSink, rrTopologyActor );
        startRestartNeededListeningActor( cluster );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink, ActorRef rrTopologyActor )
    {
        DiscoveryMember discoveryMember = discoveryMemberFactory.create( identityModule.myself() );
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Props coreTopologyProps = CoreTopologyActor.props(
                discoveryMember,
                topologySink,
                bootstrapStateSink,
                rrTopologyActor,
                replicator,
                cluster,
                topologyBuilder,
                config,
                replicatedDataMonitor,
                clusterSizeMonitor );
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        Props directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor, replicatedDataMonitor );
        return actorSystemLifecycle.applicationActorOf( directoryProps, DirectoryActor.NAME );
    }

    private ActorRef databaseStateActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> stateSink,
            ActorRef rrTopologyActor )
    {
        Props stateProps = DatabaseStateActor.props( cluster, replicator, stateSink, rrTopologyActor, replicatedDataMonitor, identityModule.memberId() );
        return actorSystemLifecycle.applicationActorOf( stateProps, DatabaseStateActor.NAME );
    }

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
            SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, databaseStateSink, receptionist, config, clock );
        return actorSystemLifecycle.applicationActorOf( readReplicaTopologyProps, ReadReplicaTopologyActor.NAME );
    }

    private void startRestartNeededListeningActor( Cluster cluster )
    {
        Runnable restart = () -> executor.execute( this::restart );
        EventStream eventStream = actorSystemLifecycle.eventStream();
        Props props = RestartNeededListeningActor.props( restart, eventStream, cluster );
        actorSystemLifecycle.applicationActorOf( props, RestartNeededListeningActor.NAME );
    }

    private void onCoreTopologyMessage( CoreTopologyMessage coreTopologyMessage )
    {
        globalTopologyState.onTopologyUpdate( coreTopologyMessage.coreTopology() );
        actorSystemLifecycle.addSeenAddresses( coreTopologyMessage.akkaMembers() );
    }

    @Override
    public void stop0() throws Exception
    {
        coreTopologyActorRef = null;
        directoryActorRef = null;

        actorSystemLifecycle.shutdown();

        globalTopologyState = newGlobalTopologyState( logProvider, listenerService );
    }

    @Override
    public void addLocalCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreTopologyForDatabase( listener.namedDatabaseId() ) );
    }

    @Override
    public void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public PublishRaftIdOutcome publishRaftId( RaftId raftId, MemberId memberId ) throws TimeoutException
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            var timeout = config.get( CausalClusteringInternalSettings.raft_id_publish_timeout );
            var request = new RaftIdSetRequest( raftId, memberId, timeout );
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
        return PublishRaftIdOutcome.FAILED_PUBLISH;
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
    public boolean canBootstrapRaftGroup( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.bootstrapState().canBootstrapRaft( namedDatabaseId );
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
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
        localLeadersByDatabaseId.remove( namedDatabaseId );
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
            log.info( "I am member %s. Updating leader info to member %s database %s and term %s", memberId(), newLeaderInfo.memberId(), namedDatabaseId.name(),
                    newLeaderInfo.term() );
            localLeadersByDatabaseId.put( namedDatabaseId, newLeaderInfo );
            sendLeaderInfoIfNeeded( newLeaderInfo, namedDatabaseId );
        }
    }

    @Override
    public void handleStepDown( long term, NamedDatabaseId namedDatabaseId )
    {
        var currentLeaderInfo = getLocalLeader( namedDatabaseId );

        var wasLeaderForTerm =
                Objects.equals( identityModule.memberId( namedDatabaseId ), currentLeaderInfo.memberId() ) &&
                term == currentLeaderInfo.term();

        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader for database %s in term %s, now moving " +
                      "to follower.", memberId(), namedDatabaseId.name(), currentLeaderInfo.term() );

            var newLeaderInfo = currentLeaderInfo.stepDown();
            localLeadersByDatabaseId.put( namedDatabaseId, newLeaderInfo );
            sendLeaderInfoIfNeeded( newLeaderInfo, namedDatabaseId );
        }
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, MemberId memberId )
    {
        var leaderInfo = localLeadersByDatabaseId.get( namedDatabaseId );
        if ( leaderInfo != null && Objects.equals( memberId, leaderInfo.memberId() ) )
        {
            return RoleInfo.LEADER;
        }
        return globalTopologyState.role( namedDatabaseId, memberId );
    }

    @Override
    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.getLeader( namedDatabaseId );
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return globalTopologyState.allCoreServers();
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.coreTopologyForDatabase( namedDatabaseId );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return globalTopologyState.allReadReplicas();
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.readReplicaTopologyForDatabase( namedDatabaseId );
    }

    @Override
    public SocketAddress lookupCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
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
    public MemberId memberId()
    {
        return identityModule.memberId();
    }

    @Override
    public DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, MemberId memberId )
    {
        return globalTopologyState.stateFor( memberId, namedDatabaseId );
    }

    @Override
    public Map<MemberId,DiscoveryDatabaseState> allCoreStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.copyOf( globalTopologyState.coreStatesForDatabase( namedDatabaseId ).memberStates() );
    }

    @Override
    public Map<MemberId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.copyOf( globalTopologyState.readReplicaStatesForDatabase( namedDatabaseId ).memberStates() );
    }

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }

    private void sendLeaderInfoIfNeeded( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId )
    {
        var directoryActor = directoryActorRef;
        if ( directoryActor != null && (leaderInfo.memberId() != null || leaderInfo.isSteppingDown()) )
        {
            directoryActor.tell( new LeaderInfoSettingMessage( leaderInfo, namedDatabaseId ), noSender() );
        }
    }

    private GlobalTopologyState newGlobalTopologyState( LogProvider logProvider, CoreTopologyListenerService listenerService )
    {
        return new GlobalTopologyState( logProvider, listenerService::notifyListeners );
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
