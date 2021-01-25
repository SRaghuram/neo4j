/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClientTopologyActor;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClusterClientManager;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Clock;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;

public class AkkaTopologyClient extends SafeLifecycle implements TopologyService
{
    private final Config config;
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final ServerSnapshotFactory serverSnapshotFactory;
    private final ServerIdentity myIdentity;
    private final LogProvider logProvider;
    private final Clock clock;
    private final JobScheduler jobScheduler;
    private final DatabaseStateService databaseStateService;

    private volatile ActorRef clientTopologyActorRef;
    private volatile GlobalTopologyState globalTopologyState;

    AkkaTopologyClient( Config config, LogProvider logProvider, ServerIdentity myIdentity, ActorSystemLifecycle actorSystemLifecycle,
            ServerSnapshotFactory serverSnapshotFactory, Clock clock, JobScheduler jobScheduler, DatabaseStateService databaseStateService )
    {
        this.config = config;
        this.myIdentity = myIdentity;
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.serverSnapshotFactory = serverSnapshotFactory;
        this.logProvider = logProvider;
        this.jobScheduler = jobScheduler;
        this.globalTopologyState = newGlobalTopologyState( logProvider, jobScheduler );
        this.clock = clock;
        this.databaseStateService = databaseStateService;
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClientActorSystem();
        startTopologyActors();
    }

    private void startTopologyActors()
    {
        var clusterClientFactory = ClusterClientManager.clusterClientProvider( actorSystemLifecycle::clusterClientSettings );
        var clusterClientManager = actorSystemLifecycle.applicationActorOf( ClusterClientManager.props( clusterClientFactory ), ClusterClientManager.NAME );

        SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbStateUpdate );
        SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink =
                actorSystemLifecycle.queueMostRecent( globalTopologyState::onRaftMappingUpdate );

        var serverSnapshot = serverSnapshotFactory.createSnapshot( databaseStateService, Map.of() );
        var clientTopologyProps = ClientTopologyActor.props(
                serverSnapshot,
                coreTopologySink,
                rrTopologySink,
                directorySink,
                databaseStateSink,
                raftMappingSink,
                clusterClientManager,
                config,
                logProvider,
                clock,
                myIdentity.serverId() );
        clientTopologyActorRef = actorSystemLifecycle.applicationActorOf( clientTopologyProps, ClientTopologyActor.NAME );
    }

    @Override
    public void stop0() throws Exception
    {
        clientTopologyActorRef = null;
        actorSystemLifecycle.shutdown();
        globalTopologyState = newGlobalTopologyState( logProvider, jobScheduler );
    }

    @Override
    public void onDatabaseStart( NamedDatabaseId namedDatabaseId )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( new DatabaseStartedMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void onRaftMemberKnown( NamedDatabaseId namedDatabaseId )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( new RaftMemberKnownMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( new DatabaseStoppedMessage( namedDatabaseId ), noSender() );
        }
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( DiscoveryDatabaseState.from( newState ), noSender() );
        }
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
        SocketAddress advertisedSocketAddress = globalTopologyState.retrieveCatchupServerAddress( upstream );
        if ( advertisedSocketAddress == null )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
        return advertisedSocketAddress;
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return globalTopologyState.role( namedDatabaseId, serverId );
    }

    @Override
    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return globalTopologyState.getLeader( namedDatabaseId );
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

    @Override
    public boolean isHealthy()
    {
        return true;
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

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }

    private static GlobalTopologyState newGlobalTopologyState( LogProvider logProvider, JobScheduler jobScheduler )
    {
        return new GlobalTopologyState( logProvider, ( ignored1, ignored2 ) ->
        {
        }, jobScheduler );
    }
}
