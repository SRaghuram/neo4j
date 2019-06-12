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
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClientTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static akka.actor.ActorRef.noSender;

public class AkkaTopologyClient extends SafeLifecycle implements TopologyService
{
    private final Config config;
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final DiscoveryMember myself;
    private final Log log;
    private final LogProvider logProvider;
    private final GlobalTopologyState globalTopologyState;

    private ActorRef clientTopologyActorRef;

    AkkaTopologyClient( Config config, LogProvider logProvider, DiscoveryMember myself, ActorSystemLifecycle actorSystemLifecycle )
    {
        this.config = config;
        this.myself = myself;
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.globalTopologyState = new GlobalTopologyState( logProvider, ignored ->
        {
        } );
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
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

        Props clientTopologyProps = ClientTopologyActor.props(
                myself,
                coreTopologySink,
                rrTopologySink,
                directorySink,
                clusterClient,
                config,
                logProvider);
        clientTopologyActorRef = actorSystemLifecycle.applicationActorOf( clientTopologyProps, ClientTopologyActor.NAME );
    }

    @Override
    public void stop0() throws Exception
    {
        clientTopologyActorRef = null;
        actorSystemLifecycle.shutdown();
    }

    @Override
    public void onDatabaseStart( DatabaseId databaseId )
    {
        clientTopologyActorRef.tell( new DatabaseStartedMessage( databaseId ), noSender() );
    }

    @Override
    public void onDatabaseStop( DatabaseId databaseId )
    {
        clientTopologyActorRef.tell( new DatabaseStoppedMessage( databaseId ), noSender() );
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
        AdvertisedSocketAddress advertisedSocketAddress = globalTopologyState.retrieveCatchupServerAddress( upstream );
        if ( advertisedSocketAddress == null )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
        return advertisedSocketAddress;
    }

    @Override
    public RoleInfo coreRole( DatabaseId databaseId, MemberId memberId )
    {
        return globalTopologyState.coreRole( databaseId, memberId );
    }

    @Override
    public MemberId memberId()
    {
        return myself.id();
    }
}
