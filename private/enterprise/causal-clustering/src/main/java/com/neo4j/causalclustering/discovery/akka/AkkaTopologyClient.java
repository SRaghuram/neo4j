/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClientTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static akka.actor.ActorRef.noSender;

public class AkkaTopologyClient extends SafeLifecycle implements TopologyService
{
    private final Config config;
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final DiscoveryMemberFactory discoveryMemberFactory;
    private final MemberId myself;
    private final LogProvider logProvider;
    private final Clock clock;

    private volatile ActorRef clientTopologyActorRef;
    private volatile GlobalTopologyState globalTopologyState;

    AkkaTopologyClient( Config config, LogProvider logProvider, MemberId myself, ActorSystemLifecycle actorSystemLifecycle,
            DiscoveryMemberFactory discoveryMemberFactory, Clock clock )
    {
        this.config = config;
        this.myself = myself;
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.discoveryMemberFactory = discoveryMemberFactory;
        this.logProvider = logProvider;
        this.globalTopologyState = newGlobalTopologyState( logProvider );
        this.clock = clock;
    }

    @Override
    public void start0()
    {
        actorSystemLifecycle.createClientActorSystem();
        startTopologyActors();
    }

    private void startTopologyActors()
    {
        ClusterClientSettings clusterClientSettings = actorSystemLifecycle.clusterClientSettings();
        ActorRef clusterClient = actorSystemLifecycle.systemActorOf( ClusterClient.props( clusterClientSettings ), "cluster-client" );

        SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> directorySink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbLeaderUpdate );
        SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink = actorSystemLifecycle.queueMostRecent( globalTopologyState::onDbStateUpdate );

        DiscoveryMember discoveryMember = discoveryMemberFactory.create( myself );

        Props clientTopologyProps = ClientTopologyActor.props(
                discoveryMember,
                coreTopologySink,
                rrTopologySink,
                directorySink,
                databaseStateSink,
                clusterClient,
                config,
                logProvider,
                clock );
        clientTopologyActorRef = actorSystemLifecycle.applicationActorOf( clientTopologyProps, ClientTopologyActor.NAME );
    }

    @Override
    public void stop0() throws Exception
    {
        clientTopologyActorRef = null;
        actorSystemLifecycle.shutdown();
        globalTopologyState = newGlobalTopologyState( logProvider );
    }

    @Override
    public void onDatabaseStart( DatabaseId databaseId )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( new DatabaseStartedMessage( databaseId ), noSender() );
        }
    }

    @Override
    public void onDatabaseStop( DatabaseId databaseId )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( new DatabaseStoppedMessage( databaseId ), noSender() );
        }
    }

    @Override
    public void stateChange( DatabaseState newState )
    {
        var clientTopologyActor = clientTopologyActorRef;
        if ( clientTopologyActor != null )
        {
            clientTopologyActor.tell( newState, noSender() );
        }
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
        SocketAddress advertisedSocketAddress = globalTopologyState.retrieveCatchupServerAddress( upstream );
        if ( advertisedSocketAddress == null )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
        return advertisedSocketAddress;
    }

    @Override
    public RoleInfo role( DatabaseId databaseId, MemberId memberId )
    {
        return globalTopologyState.role( databaseId, memberId );
    }

    @Override
    public MemberId memberId()
    {
        return myself;
    }

    @Override
    public DatabaseState stateFor( DatabaseId databaseId, MemberId memberId )
    {
        return globalTopologyState.stateFor( memberId, databaseId );
    }

    @Override
    public ReplicatedDatabaseState coreStatesForDatabase( DatabaseId databaseId )
    {
        return globalTopologyState.coreStatesForDatabase( databaseId );
    }

    @Override
    public ReplicatedDatabaseState readReplicaStatesForDatabase( DatabaseId databaseId )
    {
        return globalTopologyState.readReplicaStatesForDatabase( databaseId );
    }

    @VisibleForTesting
    GlobalTopologyState topologyState()
    {
        return globalTopologyState;
    }

    private static GlobalTopologyState newGlobalTopologyState( LogProvider logProvider )
    {
        return new GlobalTopologyState( logProvider, ignored ->
        {
        } );
    }
}
