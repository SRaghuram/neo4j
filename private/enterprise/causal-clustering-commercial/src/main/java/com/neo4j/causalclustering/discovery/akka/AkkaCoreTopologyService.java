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
import akka.pattern.AskTimeoutException;
import akka.pattern.Patterns;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterIdActor.PublishClusterIdOutcome;
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterIdSetRequest;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.AbstractCoreTopologyService;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.DiscoveryTimeoutException;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;
import static java.lang.String.format;

public class AkkaCoreTopologyService extends AbstractCoreTopologyService
{
    private Optional<ActorRef> coreTopologyActorRef = Optional.empty();
    private Optional<ActorRef> directoryActorRef = Optional.empty();
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final RetryStrategy catchupAddressRetryStrategy;
    private final TopologyState topologyState;
    private final RetryStrategy restartRetryStrategy;
    private final ExecutorService executor;
    private final Clock clock;
    private final ReplicatedDataMonitor replicatedDataMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;
    private volatile LeaderInfo leaderInfo = LeaderInfo.INITIAL;

    public AkkaCoreTopologyService( Config config, MemberId myself, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
            LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, RetryStrategy restartRetryStrategy,
            ExecutorService executor, Clock clock, Monitors monitors )
    {
        super( config, myself, logProvider, userLogProvider );
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.restartRetryStrategy = restartRetryStrategy;
        this.executor = executor;
        this.clock = clock;
        this.replicatedDataMonitor = monitors.newMonitor( ReplicatedDataMonitor.class );
        this.clusterSizeMonitor = monitors.newMonitor( ClusterSizeMonitor.class );
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
                config,
                replicatedDataMonitor,
                clusterSizeMonitor );
        return actorSystemLifecycle.applicationActorOf( coreTopologyProps, CoreTopologyActor.NAME );
    }

    private ActorRef directoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink,
            ActorRef rrTopologyActor )
    {
        Props directoryProps = DirectoryActor.props( cluster, replicator, directorySink, rrTopologyActor, replicatedDataMonitor );
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
    public boolean setClusterId( ClusterId clusterId, String dbName ) throws DiscoveryTimeoutException
    {
        if ( coreTopologyActorRef.isPresent() )
        {
            ActorRef actor = coreTopologyActorRef.get();
            Duration timeout = config.get( CausalClusteringSettings.cluster_id_publish_timeout );
            ClusterIdSetRequest clusterIdSetRequest = new ClusterIdSetRequest( clusterId, dbName, timeout );
            log.info( "Attempting to set ClusterId with request %s", clusterIdSetRequest );
            CompletionStage<Object> idSet = Patterns.ask( actor, clusterIdSetRequest, timeout );
            CompletableFuture<PublishClusterIdOutcome> idSetJob = idSet.thenApply( response ->
            {
                if ( !(response instanceof PublishClusterIdOutcome) )
                {
                    throw new IllegalArgumentException(
                            format( "Unexpected response when attempting to publish cluster Id. Expected PublishClusterIdOutcome, received %s",
                            response.getClass().getCanonicalName() ) );
                }

                return (PublishClusterIdOutcome) response;
            } ).toCompletableFuture();

            try
            {
                PublishClusterIdOutcome outcome = idSetJob.join();

                if ( outcome == PublishClusterIdOutcome.TIMEOUT )
                {
                    log.warn( "Attempt to publish the clusterId to all other discovery members timed out" );
                    throw new DiscoveryTimeoutException();
                }

                return outcome == PublishClusterIdOutcome.SUCCESS;
            }
            catch ( CompletionException e )
            {
                if ( e.getCause() instanceof AskTimeoutException )
                {
                    log.warn( "Attempt validate published clusterId against other discovery members timed out" );
                    throw new DiscoveryTimeoutException( e );
                }
                log.error( e.getCause().getMessage() );
                return false;
            }
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
            log.warn( "Not attempting to restart discovery system because it is not currently running. Its state is %s", state() );
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
    public void publishLeader( LeaderInfo leaderInfo )
    {
        this.leaderInfo = leaderInfo;
        if ( leaderInfo.memberId() != null || leaderInfo.isSteppingDown() )
        {
            directoryActorRef.ifPresent( actor -> actor.tell( new LeaderInfoSettingMessage( leaderInfo, localDBName() ), noSender() ) );
        }
    }

    @Override
    public void publishStepDown( LeaderInfo steppingDown )
    {
        publishLeader( steppingDown );
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
            return catchupAddressRetryStrategy.apply( () -> topologyState.retrieveSocketAddress( upstream ), Objects::nonNull );
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
