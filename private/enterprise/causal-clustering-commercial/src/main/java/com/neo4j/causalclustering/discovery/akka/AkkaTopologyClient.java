/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClientTopologyActor;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;

import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class AkkaTopologyClient extends SafeLifecycle implements TopologyService
{
    private final Config config;
    private final ActorSystemLifecycle actorSystemLifecycle;
    private final MemberId myself;
    private final Log log;
    private final LogProvider logProvider;
    private final TopologyState topologyState;

    public AkkaTopologyClient( Config config, LogProvider logProvider, MemberId myself, ActorSystemLifecycle actorSystemLifecycle )
    {
        this.config = config;
        this.myself = myself;
        this.actorSystemLifecycle = actorSystemLifecycle;
        this.topologyState = new TopologyState( config, logProvider, ignored -> {} );
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
    }

    @Override
    public void init0()
    {
        actorSystemLifecycle.createClientActorSystem();
    }

    @Override
    public void start0()
    {
        startTopologyActors();
    }

    private void startTopologyActors()
    {
        ClusterClientSettings clusterClientSettings = actorSystemLifecycle.clusterClientSettings();
        ActorRef clusterClient = actorSystemLifecycle.systemActorOf( ClusterClient.props( clusterClientSettings ), "cluster-client" );

        SourceQueueWithComplete<CoreTopology> coreTopologySink = actorSystemLifecycle.queueMostRecent( topologyState::onTopologyUpdate );
        SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink = actorSystemLifecycle.queueMostRecent( topologyState::onTopologyUpdate );
        SourceQueueWithComplete<Map<String,LeaderInfo>> directorySink = actorSystemLifecycle.queueMostRecent( topologyState::onDbLeaderUpdate );

        Props clientTopologyProps = ClientTopologyActor.props(
                myself,
                coreTopologySink,
                rrTopologySink,
                directorySink,
                clusterClient,
                config,
                logProvider);
        actorSystemLifecycle.applicationActorOf( clientTopologyProps, ClientTopologyActor.NAME );
    }

    @Override
    public void stop0()
    {
        actorSystemLifecycle.stop();
    }

    @Override
    public void shutdown0() throws Throwable
    {
        actorSystemLifecycle.shutdown();
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
    public ReadReplicaTopology allReadReplicas()
    {
        return topologyState.readReplicaTopology();
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return topologyState.localCoreTopology();
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return topologyState.localReadReplicaTopology();
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return topologyState.retrieveSocketAddress( upstream );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return topologyState.allCoreRoles();
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }
}
