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
import com.neo4j.causalclustering.discovery.AbstractCoreTopologyService;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftIdSettingMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;

public class AkkaCoreTopologyService extends AbstractCoreTopologyService
{
    private Optional<ActorRef> coreTopologyActorRef = Optional.empty();
    private Optional<ActorRef> directoryActorRef = Optional.empty();
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    private final RetryStrategy catchupAddressRetryStrategy;
    private final RetryStrategy restartRetryStrategy;
    private final GlobalTopologyState globalTopologyState;
    private final Executor executor;
    private final Clock clock;

    public AkkaCoreTopologyService( Config config, DiscoveryMember myself, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, RetryStrategy restartRetryStrategy,
            Executor executor, Clock clock )
    {
        super( config, myself, logProvider, userLogProvider );
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restartRetryStrategy = restartRetryStrategy;
        this.executor = executor;
        this.clock = clock;
        this.globalTopologyState = new GlobalTopologyState( logProvider, listenerService::notifyListeners );
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
        ActorRef coreTopologyActor = coreTopologyActor( cluster, replicator, coreTopologySink, bootstrapStateSink, rrTopologyActor );
        ActorRef directoryActor = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        startRestartNeededListeningActor( cluster );

        coreTopologyActorRef = Optional.of( coreTopologyActor );
        directoryActorRef = Optional.of( directoryActor );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink, ActorRef rrTopologyActor )
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder( cluster.selfUniqueAddress(), logProvider );
        Props coreTopologyProps = CoreTopologyActor.props(
                myself,
                topologySink,
                bootstrapStateSink,
                rrTopologyActor,
                replicator,
                cluster,
                topologyBuilder,
                config,
                logProvider );
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        Props directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor, logProvider );
        return actorSystemLifecycle.applicationActorOf( directoryProps, DirectoryActor.NAME );
    }

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, receptionist, logProvider, config, clock );
        return actorSystemLifecycle.applicationActorOf( readReplicaTopologyProps, ReadReplicaTopologyActor.NAME );
    }

    private ActorRef startRestartNeededListeningActor( Cluster cluster )
    {
        Runnable restart = () -> executor.execute( this::restart );
        EventStream eventStream = actorSystemLifecycle.eventStream();
        Props props = RestartNeededListeningActor.props( restart, eventStream, cluster, logProvider );
        return actorSystemLifecycle.applicationActorOf( props, RestartNeededListeningActor.NAME );
    }

    private void onCoreTopologyMessage( CoreTopologyMessage coreTopologyMessage )
    {
        this.globalTopologyState.onTopologyUpdate( coreTopologyMessage.coreTopology() );
        actorSystemLifecycle.addSeenAddresses( coreTopologyMessage.akkaMembers() );
    }

    @Override
    public void stop0() throws Exception
    {
        coreTopologyActorRef = Optional.empty();
        directoryActorRef = Optional.empty();

        actorSystemLifecycle.shutdown();
    }

    @Override
    public boolean setRaftId( RaftId raftId, DatabaseId databaseId )
    {
        if ( coreTopologyActorRef.isPresent() )
        {
            ActorRef actor = coreTopologyActorRef.get();
            actor.tell( new RaftIdSettingMessage( raftId, databaseId ), noSender() );
            return true;
        }
        else
        {
            return false;
        }
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
    public void setLeader0( LeaderInfo leaderInfo, DatabaseId databaseId )
    {
        if ( leaderInfo.memberId() != null || leaderInfo.isSteppingDown() )
        {
            directoryActorRef.ifPresent( actor -> actor.tell( new LeaderInfoSettingMessage( leaderInfo, databaseId ), noSender() ) );
        }
    }

    @Override
    public void handleStepDown0( LeaderInfo steppingDown, DatabaseId databaseId )
    {
        setLeader0( steppingDown, databaseId );
    }

    @Override
    protected RoleInfo coreRole0( DatabaseId databaseId, MemberId memberId )
    {
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
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
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

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }
}
