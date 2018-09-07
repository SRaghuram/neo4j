/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClientTopologyActor extends AbstractActorWithTimers
{
    static final String TARGET_PATH = "/user/" + ReadReplicatorTopologyActor.NAME;
    private static final String REFRESH = "topology refresh";

    public static Props props( MemberId myself, SourceQueueWithComplete<CoreTopology> coreTopologySink,
            SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink, SourceQueueWithComplete<Map<String,LeaderInfo>> discoverySink, ActorRef clusterClient,
            Config config, LogProvider logProvider )
    {
        return Props.create( ClientTopologyActor.class,
                () -> new ClientTopologyActor( myself, coreTopologySink, rrTopologySink, discoverySink, clusterClient, config, logProvider ) );
    }

    public static final String NAME = "cc-client-topology-actor";

    private final MemberId myself;
    private final ReadReplicaInfo readReplicaInfo;
    private final SourceQueueWithComplete<CoreTopology> coreTopologySink;
    private final SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink;
    private final SourceQueueWithComplete<Map<String,LeaderInfo>> discoverySink;
    private final ActorRef clusterClient;
    private final Config config;
    private final Log log;

    ClientTopologyActor( MemberId myself, SourceQueueWithComplete<CoreTopology> coreTopologySink, SourceQueueWithComplete<ReadReplicaTopology> rrTopologySink,
            SourceQueueWithComplete<Map<String,LeaderInfo>> discoverySink, ActorRef clusterClient, Config config, LogProvider logProvider )
    {
        this.myself = myself;
        this.coreTopologySink = coreTopologySink;
        this.rrTopologySink = rrTopologySink;
        this.discoverySink = discoverySink;
        this.clusterClient = clusterClient;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
        this.readReplicaInfo = ReadReplicaInfo.from( config );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( CoreTopology.class, coreTopologySink::offer )
                .match( ReadReplicaTopology.class, rrTopologySink::offer )
                .match( LeaderInfoDirectoryMessage.class, msg -> discoverySink.offer( msg.leaders() ) )
                .match( Refresh.class, ignored -> sendInfo() )
                .build();
    }

    @Override
    public void preStart()
    {
        getTimers().startPeriodicTimer( REFRESH, Refresh.instance, config.get( CausalClusteringSettings.cluster_topology_refresh ) );
        sendInfo();
    }

    private void sendInfo()
    {
        ReadReplicaInfoMessage msg = new ReadReplicaInfoMessage( readReplicaInfo, myself, clusterClient, getSelf() );
        sendToCore( msg );
    }

    @Override
    public void postStop()
    {
        ReadReplicaRemovalMessage msg = new ReadReplicaRemovalMessage( clusterClient );
        sendToCore( msg );
    }

    private void sendToCore( Object msg )
    {
        clusterClient.tell( new ClusterClient.Send( TARGET_PATH, msg ), getSelf() );
    }

    private static final class Refresh
    {
        private static final Refresh instance = new Refresh();
        private Refresh()
        {
        }

        public static Refresh getInstance()
        {
            return instance;
        }
    }
}
