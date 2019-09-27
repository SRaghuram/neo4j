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
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterIdSettingMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.AbstractCoreTopologyService;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;

public class AkkaCoreTopologyService extends AbstractCoreTopologyService
{
    private Optional<ActorRef> coreTopologyActorRef = Optional.empty();
    private Optional<ActorRef> directoryActorRef = Optional.empty();
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final TopologyServiceRetryStrategy catchupAddressRetryStrategy;
    private final TopologyState topologyState;
    private final RetryStrategy<Void,Boolean> restartRetryStrategy;
    private final ExecutorService executor;
    private final Clock clock;
    private volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;

    public AkkaCoreTopologyService( Config config, MemberId myself, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, TopologyServiceRetryStrategy catchupAddressRetryStrategy, RetryStrategy<Void,Boolean> restartRetryStrategy,
            ExecutorService executor, Clock clock )
    {
        super( config, myself, logProvider, userLogProvider );
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restartRetryStrategy = restartRetryStrategy;
        this.executor = executor;
        this.clock = clock;
        this.topologyState = new TopologyState( config, logProvider, listenerService::notifyListeners );
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClusterActorSystem();

        SourceQueueWithComplete<CoreTopologyMessage> coreTopologySink = actorSystemLifecycle.queueMostRecent( this::onCoreTopologyMessage );
        SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( topologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink = actorSystemLifecycle.queueMostRecent( topologyState::onDbLeaderUpdate );

        Cluster cluster = actorSystemLifecycle.cluster();
        ActorRef replicator = actorSystemLifecycle.replicator();
        ActorRef rrTopologyActor = readReplicaTopologyActor( rrTopologySink );
        ActorRef coreTopologyActor = coreTopologyActor( cluster, replicator, coreTopologySink, rrTopologyActor );
        ActorRef directoryActor = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        startRestartNeededListeningActor( cluster );

        coreTopologyActorRef = Optional.of( coreTopologyActor );
        directoryActorRef = Optional.of( directoryActor );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            ActorRef rrTopologyActor )
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder( config, cluster.selfUniqueAddress() );
        Props coreTopologyProps = CoreTopologyActor.props(
                myself,
                topologySink,
                rrTopologyActor,
                replicator,
                cluster,
                topologyBuilder,
                config );
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        Props directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor );
        return actorSystemLifecycle.applicationActorOf( directoryProps, DirectoryActor.NAME );
    }

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<ReadReplicaTopology> topologySink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, receptionist, config, clock );
        return actorSystemLifecycle.applicationActorOf( readReplicaTopologyProps, ReadReplicaTopologyActor.NAME );
    }

    private ActorRef startRestartNeededListeningActor( Cluster cluster )
    {
        Runnable restart = () -> executor.submit( this::restart );
        EventStream eventStream = actorSystemLifecycle.eventStream();
        Props props = RestartNeededListeningActor.props( restart, eventStream, cluster );
        return actorSystemLifecycle.applicationActorOf( props, RestartNeededListeningActor.NAME );
    }

    private void onCoreTopologyMessage( CoreTopologyMessage coreTopologyMessage )
    {
        this.topologyState.onTopologyUpdate( coreTopologyMessage.coreTopology() );
        actorSystemLifecycle.addSeenAddresses( coreTopologyMessage.akkaMembers() );
    }

    @Override
    public void stop0() throws Throwable
    {
        coreTopologyActorRef = Optional.empty();
        directoryActorRef = Optional.empty();

        actorSystemLifecycle.shutdown();
    }

    @Override
    public boolean setClusterId( ClusterId clusterId, String dbName )
    {
        if ( coreTopologyActorRef.isPresent() )
        {
            ActorRef actor = coreTopologyActorRef.get();
            actor.tell( new ClusterIdSettingMessage( clusterId, dbName ), noSender() );
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public LeaderInfo getLeader()
    {
        return leaderInfo;
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

        restartRetryStrategy.apply( null, this::doRestart, r -> r );
    }

    private boolean doRestart( Void ignored )
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
            userLog.error( "Failed to restart discovery system", t );
            return false;
        }
    }

    @Override
    public void setLeader0( LeaderInfo leaderInfo )
    {
        this.leaderInfo = leaderInfo;
        if ( leaderInfo.memberId() != null || leaderInfo.isSteppingDown() )
        {
            directoryActorRef.ifPresent( actor -> actor.tell( new LeaderInfoSettingMessage( leaderInfo, localDBName() ), noSender() ) );
        }
    }

    @Override
    public void handleStepDown0( LeaderInfo steppingDown )
    {
        setLeader0( steppingDown );
    }

    @Override
    public String localDBName()
    {
        return topologyState.localDBName();
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return topologyState.coreTopology();
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return topologyState.localCoreTopology();
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return topologyState.readReplicaTopology();
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return topologyState.localReadReplicaTopology();
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return catchupAddressRetryStrategy.apply( upstream, topologyState::retrieveSocketAddress, Optional::isPresent );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return topologyState.allCoreRoles();
    }

    @VisibleForTesting
    TopologyState topologyState()
    {
        return topologyState;
    }
}
