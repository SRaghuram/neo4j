/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopologyListenerService;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyActor;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreTopologyMessage;
import com.neo4j.causalclustering.discovery.akka.coretopology.TopologyBuilder;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseStateActor;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.DirectoryActor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.DbmsPanicEvent;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;
import static com.neo4j.causalclustering.error_handling.DbmsPanicReason.IrrecoverableDiscoveryFailure;

public abstract class AkkaMemberTopologyService extends SafeLifecycle implements TopologyService, Restartable
{
    protected final ActorSystemLifecycle actorSystemLifecycle;
    private final LogProvider logProvider;
    protected final RetryStrategy catchupAddressRetryStrategy;
    protected final ActorSystemRestarter actorSystemRestarter;
    protected final ServerSnapshotFactory serverSnapshotFactory;
    protected final JobScheduler jobScheduler;
    protected final CallableExecutor executor;
    private final Clock clock;
    private final ReplicatedDataMonitor replicatedDataMonitor;
    private final ClusterSizeMonitor clusterSizeMonitor;
    protected final DatabaseStateService databaseStateService;
    protected final Config config;
    protected final ServerIdentity myIdentity;
    protected final Log log;
    protected final Log userLog;

    protected final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    protected final Panicker panicker;

    protected volatile boolean isRestarting;
    protected volatile ActorRef coreTopologyActorRef;
    protected volatile ActorRef directoryActorRef;
    protected volatile ActorRef databaseStateActorRef;
    protected volatile GlobalTopologyState globalTopologyState;

    public AkkaMemberTopologyService( Config config, ServerIdentity myIdentity, ActorSystemLifecycle actorSystemLifecycle, LogProvider logProvider,
                                      LogProvider userLogProvider, RetryStrategy catchupAddressRetryStrategy, ActorSystemRestarter actorSystemRestarter,
                                      ServerSnapshotFactory serverSnapshotFactory, JobScheduler jobScheduler, Clock clock, Monitors monitors,
                                      DatabaseStateService databaseStateService, Panicker panicker )
    {
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.logProvider = logProvider;
        this.catchupAddressRetryStrategy = catchupAddressRetryStrategy;
        this.actorSystemRestarter = actorSystemRestarter;
        this.serverSnapshotFactory = serverSnapshotFactory;
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
        this.panicker = panicker;
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClusterActorSystem(this);
        var coreTopologySink = actorSystemLifecycle.queueMostRecent( this::onCoreTopologyMessage );
        var directorySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        var bootstrapStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onBootstrapStateUpdate );
        var databaseStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbStateUpdate );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        var raftMappingSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onRaftMappingUpdate );

        var cluster = actorSystemLifecycle.cluster();
        var replicator = actorSystemLifecycle.replicator();
        var rrTopologyActor = readReplicaTopologyActor( rrTopologySink, databaseStateSink );
        databaseStateActorRef = databaseStateActor( cluster, replicator, databaseStateSink, rrTopologyActor );
        coreTopologyActorRef =
                coreTopologyActor( cluster, replicator, coreTopologySink, bootstrapStateSink, raftMappingSink, databaseStateActorRef, rrTopologyActor );
        directoryActorRef = directoryActor( cluster, replicator, directorySink, rrTopologyActor );
        publishInitialData( coreTopologyActorRef, directoryActorRef, databaseStateActorRef );
    }

    private ActorRef coreTopologyActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<CoreTopologyMessage> topologySink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink, SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink,
            ActorRef databaseStateActor, ActorRef rrTopologyActor )
    {
        var topologyBuilder = new TopologyBuilder();
        var coreTopologyProps = CoreTopologyActor.props(
                topologySink,
                bootstrapStateSink,
                raftMappingSink,
                databaseStateActor,
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
        var serverSnapshot = createServerSnapshot();
        for ( ActorRef actorRef : actorRefs )
        {
            actorRef.tell( new PublishInitialData( serverSnapshot ), noSender() );
        }
    }

    protected abstract ServerSnapshot createServerSnapshot();

    protected abstract void removeLeaderInfo( NamedDatabaseId namedDatabaseId );

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
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return globalTopologyState.resolveRaftMemberForServer( namedDatabaseId.databaseId(), serverId );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId raftMemberId )
    {
        return globalTopologyState.resolveServerForRaftMember( raftMemberId );
    }

    @Override
    public Future<?> scheduleRestart()
    {
        if ( isRestarting )
        {
            log.warn( "Core topology service restart requested when restart already in progress." );
        }
        return executor.submit( this::restartSameThread );
    }

    @VisibleForTesting
    public synchronized boolean restartSameThread()
    {
        if ( !SafeLifecycle.State.RUN.equals( state() ) )
        {
            log.info( "Not restarting because not running. State is %s", state() );
            return false;
        }

        userLog.info( "Restarting discovery system after probable network partition" );

        try
        {
            actorSystemRestarter.restart( "Discovery system", this::doRestart );
            return true;
        }
        catch ( Throwable e )
        {
            log.error( "Unable to restart discovery system. Triggering Dbms panic." );
            panicker.panic( new DbmsPanicEvent( IrrecoverableDiscoveryFailure, e ) );
            return false;
        }
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
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
        removeLeaderInfo( namedDatabaseId );
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
    public abstract RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId );

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

    private GlobalTopologyState newGlobalTopologyState( LogProvider logProvider, CoreTopologyListenerService listenerService )
    {
        return new GlobalTopologyState( logProvider, listenerService::notifyListeners, jobScheduler );
    }

    @Override
    public boolean isHealthy()
    {
        return actorSystemRestarter.isHealthy();
    }

    @VisibleForTesting
    public Cluster getAkkaCluster()
    {
        return actorSystemLifecycle.cluster();
    }
}
