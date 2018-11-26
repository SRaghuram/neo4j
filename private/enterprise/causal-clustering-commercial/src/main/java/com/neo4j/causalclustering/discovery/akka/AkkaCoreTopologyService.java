/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterIdSettingMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.AbstractCoreTopologyService;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

public class AkkaCoreTopologyService extends AbstractCoreTopologyService
{
    private Optional<ActorRef> coreTopologyActorRef = Optional.empty();
    private Optional<ActorRef> directoryActorRef = Optional.empty();
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    private final RetryStrategy retryStrategy;
    private final TopologyState topologyState;
    private final Clock clock;
    private volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;

    public AkkaCoreTopologyService( Config config, MemberId myself, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy topologyServiceRetryStrategy, Clock clock )
    {
        super( config, myself, logProvider, userLogProvider );
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.retryStrategy = topologyServiceRetryStrategy;
        this.clock = clock;
        this.topologyState = new TopologyState( config, logProvider, listenerService::notifyListeners );
    }

    @Override
    public void init0()
    {
        actorSystemLifecycle.createClusterActorSystem();
    }

    @Override
    public void start0()
    {
        startTopologyActors();
    }

    private void startTopologyActors()
    {
        SourceQueueWithComplete<CoreTopology> coreTopologySink = actorSystemLifecycle.queueMostRecent( topologyState::onTopologyUpdate );
        SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( topologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink = actorSystemLifecycle.queueMostRecent( topologyState::onDbLeaderUpdate );

        Cluster cluster = actorSystemLifecycle.cluster();
        ActorRef replicator = actorSystemLifecycle.replicator();
        ActorRef rrTopologyActor = readReplicaTopologyActor( rrTopologySink );
        ActorRef coreTopologyActor = coreTopologyActor( cluster, replicator, coreTopologySink, rrTopologyActor );
        ActorRef directoryActor = directoryActor( cluster, replicator, directorySink, rrTopologyActor );

        coreTopologyActorRef = Optional.of( coreTopologyActor );
        directoryActorRef = Optional.of( directoryActor );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopology> topologySink, ActorRef rrTopologyActor )
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder( config, cluster.selfUniqueAddress(), logProvider );
        Props coreTopologyProps = CoreTopologyActor.props(
                myself,
                topologySink,
                rrTopologyActor,
                replicator,
                cluster,
                topologyBuilder,
                config,
                logProvider);
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        Props directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor, logProvider );
        return actorSystemLifecycle.applicationActorOf( directoryProps, DirectoryActor.NAME );
    }

    private ActorRef readReplicaTopologyActor( SourceQueueWithComplete<ReadReplicaTopology> topologySink )
    {
        ClusterClientReceptionist receptionist = actorSystemLifecycle.clusterClientReceptionist();
        Props readReplicaTopologyProps = ReadReplicaTopologyActor.props( topologySink, receptionist, logProvider, config, clock );
        return actorSystemLifecycle.applicationActorOf( readReplicaTopologyProps, ReadReplicaTopologyActor.NAME );
    }

    @Override
    public void stop0()
    {
        coreTopologyActorRef = Optional.empty();
        directoryActorRef = Optional.empty();
    }

    @Override
    public void shutdown0() throws Throwable
    {
        actorSystemLifecycle.shutdown();
    }

    @Override
    public boolean setClusterId( ClusterId clusterId, String dbName )
    {
        if ( coreTopologyActorRef.isPresent() )
        {
            ActorRef actor = coreTopologyActorRef.get();
            actor.tell( new ClusterIdSettingMessage( clusterId, dbName ), ActorRef.noSender() );
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

    @Override
    public void setLeader0( LeaderInfo leaderInfo )
    {
        this.leaderInfo = leaderInfo;
        if ( leaderInfo.memberId() != null || leaderInfo.isSteppingDown() )
        {
            directoryActorRef.ifPresent( actor -> actor.tell( new LeaderInfoSettingMessage( leaderInfo, localDBName() ), ActorRef.noSender() ) );
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
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        try
        {
            return retryStrategy.apply( () -> topologyState.retrieveSocketAddress( upstream ), Objects::nonNull );
        }
        catch ( TimeoutException e )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
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
