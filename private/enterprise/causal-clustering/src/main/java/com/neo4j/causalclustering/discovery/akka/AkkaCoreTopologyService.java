/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.event.EventStream;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyListenerService;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftIdSettingMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;

public class AkkaCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    private final RetryStrategy catchupAddressRetryStrategy;
    private final RetryStrategy restartRetryStrategy;
    private final DiscoveryMemberFactory discoveryMemberFactory;
    private final Executor executor;
    private final Clock clock;
    private final ReplicatedDataMonitor replicatedDataMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final Log userLog;

    private final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    private final Map<DatabaseId,LeaderInfo> localLeadersByDatabaseId = new ConcurrentHashMap<>();

    private volatile ActorRef coreTopologyActorRef;
    private volatile ActorRef directoryActorRef;
    private volatile GlobalTopologyState globalTopologyState;

    public AkkaCoreTopologyService( Config config, MemberId myself, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, RetryStrategy restartRetryStrategy,
            DiscoveryMemberFactory discoveryMemberFactory, Executor executor, Clock clock, Monitors monitors )
    {
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restartRetryStrategy = restartRetryStrategy;
        this.discoveryMemberFactory = discoveryMemberFactory;
        this.executor = executor;
        this.clock = clock;
        this.config = config;
        this.myself = myself;
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
        SourceQueueWithComplete<CoreTopologyMessage> coreTopologySink = actorSystemLifecycle.queueMostRecent( this::onCoreTopologyMessage );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        SourceQueueWithComplete<BootstrapState> bootstrapStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onBootstrapStateUpdate );

        Cluster cluster = actorSystemLifecycle.cluster();
        ActorRef replicator = actorSystemLifecycle.replicator();
        ActorRef rrTopologyActor = readReplicaTopologyActor( rrTopologySink );
        coreTopologyActorRef = coreTopologyActor( cluster, replicator, coreTopologySink, bootstrapStateSink, rrTopologyActor );
        directoryActorRef = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        startRestartNeededListeningActor( cluster );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink, ActorRef rrTopologyActor )
    {
        DiscoveryMember discoveryMember = discoveryMemberFactory.create( myself );
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

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, receptionist, config, clock );
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
        listener.onCoreTopologyChange( coreTopologyForDatabase( listener.databaseId() ) );
    }

    @Override
    public void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public boolean setRaftId( RaftId raftId, DatabaseId databaseId )
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new RaftIdSettingMessage( raftId, databaseId ), noSender() );
            return true;
        }
        return false;
    }

    @Override
    public boolean canBootstrapRaftGroup( DatabaseId databaseId )
    {
        return globalTopologyState.bootstrapState().canBootstrapRaft( databaseId );
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

        try
        {
            restartRetryStrategy.apply( this::doRestart, r -> r );
        }
        catch ( TimeoutException e )
        {
            log.error( "Unable to restart discovery system", e );
            throw new IllegalStateException( e );
        }
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
    public void onDatabaseStart( DatabaseId databaseId )
    {
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new DatabaseStartedMessage( databaseId ), noSender() );
        }
    }

    @Override
    public void onDatabaseStop( DatabaseId databaseId )
    {
        localLeadersByDatabaseId.remove( databaseId );
        var coreTopologyActor = coreTopologyActorRef;
        if ( coreTopologyActor != null )
        {
            coreTopologyActor.tell( new DatabaseStoppedMessage( databaseId ), noSender() );
        }
    }

    @Override
    public void setLeader( LeaderInfo newLeaderInfo, DatabaseId databaseId )
    {
        var currentLeaderInfo = getLocalLeader( databaseId );
        if ( currentLeaderInfo.term() < newLeaderInfo.term() )
        {
            log.info( "Leader %s updating leader info for database %s and term %s", memberId(), databaseId.name(), newLeaderInfo.term() );
            localLeadersByDatabaseId.put( databaseId, newLeaderInfo );
            sendLeaderInfoIfNeeded( newLeaderInfo, databaseId );
        }
    }

    @Override
    public void handleStepDown( long term, DatabaseId databaseId )
    {
        var currentLeaderInfo = getLocalLeader( databaseId );

        var wasLeaderForTerm =
                Objects.equals( memberId(), currentLeaderInfo.memberId() ) &&
                term == currentLeaderInfo.term();

        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader for database %s in term %s, now moving " +
                      "to follower.", memberId(), databaseId.name(), currentLeaderInfo.term() );

            var newLeaderInfo = currentLeaderInfo.stepDown();
            localLeadersByDatabaseId.put( databaseId, newLeaderInfo );
            sendLeaderInfoIfNeeded( newLeaderInfo, databaseId );
        }
    }

    @Override
    public RoleInfo coreRole( DatabaseId databaseId, MemberId memberId )
    {
        var leaderInfo = localLeadersByDatabaseId.get( databaseId );
        if ( leaderInfo != null && Objects.equals( memberId, leaderInfo.memberId() ) )
        {
            return RoleInfo.LEADER;
        }
        return globalTopologyState.coreRole( databaseId, memberId );
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return globalTopologyState.allCoreServers();
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return globalTopologyState.coreTopologyForDatabase( databaseId );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return globalTopologyState.allReadReplicas();
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return globalTopologyState.readReplicaTopologyForDatabase( databaseId );
    }

    @Override
    public SocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
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
        return myself;
    }

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }

    private void sendLeaderInfoIfNeeded( LeaderInfo leaderInfo, DatabaseId databaseId )
    {
        var directoryActor = directoryActorRef;
        if ( directoryActor != null && (leaderInfo.memberId() != null || leaderInfo.isSteppingDown()) )
        {
            directoryActor.tell( new LeaderInfoSettingMessage( leaderInfo, databaseId ), noSender() );
        }
    }

    private GlobalTopologyState newGlobalTopologyState( LogProvider logProvider, CoreTopologyListenerService listenerService )
    {
        return new GlobalTopologyState( logProvider, listenerService::notifyListeners );
    }

    private LeaderInfo getLocalLeader( DatabaseId databaseId )
    {
        return localLeadersByDatabaseId.getOrDefault( databaseId, LeaderInfo.INITIAL );
    }
}
