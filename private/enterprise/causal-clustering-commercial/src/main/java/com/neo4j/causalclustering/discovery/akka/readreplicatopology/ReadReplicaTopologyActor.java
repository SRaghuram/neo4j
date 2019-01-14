/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClientReceptionist;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ReadReplicaTopologyActor extends AbstractActor
{
    private final SourceQueueWithComplete<ReadReplicaTopology> topologySink;
    private final Log log;

    private CoreTopology coreTopology = CoreTopology.EMPTY;
    private LeaderInfoDirectoryMessage databaseLeaderInfo = LeaderInfoDirectoryMessage.EMPTY;
    private ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;

    private Set<ActorRef> myClusterClients = new HashSet<>();
    private ReadReplicaViewMessage readReplicaViewMessage = ReadReplicaViewMessage.EMPTY;

    public static Props props( SourceQueueWithComplete<ReadReplicaTopology> topologySink, ClusterClientReceptionist receptionist, LogProvider logProvider,
            Config config, Clock clock )
    {
        return Props.create( ReadReplicaTopologyActor.class,
                () -> new ReadReplicaTopologyActor( topologySink, receptionist, logProvider, config, clock ) );
    }

    public static final String NAME = "cc-rr-topology-actor";

    ReadReplicaTopologyActor( SourceQueueWithComplete<ReadReplicaTopology> topologySink, ClusterClientReceptionist receptionist, LogProvider logProvider,
            Config config, Clock clock )
    {
        this.topologySink = topologySink;
        this.log = logProvider.getLog( getClass() );

        Duration refresh = config.get( CausalClusteringSettings.cluster_topology_refresh );
        Props readReplicaViewProps = ReadReplicaViewActor.props( getSelf(), receptionist, clock, refresh, logProvider );
        getContext().actorOf( readReplicaViewProps );

        Props clusterClientViewProps = ClusterClientViewActor.props( getSelf(), receptionist.underlying(), logProvider );
        getContext().actorOf( clusterClientViewProps );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( ClusterClientViewMessage.class,     this::handleClusterClientView )
                .match( ReadReplicaViewMessage.class,       this::handleReadReplicaView )
                .match( ReadReplicaViewActor.Tick.class,    this::sendTopologiesToClients )
                .match( CoreTopology.class,                 this::setCoreTopology )
                .match( LeaderInfoDirectoryMessage.class,   this::setDatabaseLeaderInfo )
                .build();
    }

    private void handleReadReplicaView( ReadReplicaViewMessage msg )
    {
        readReplicaViewMessage = msg;
        buildTopology();
    }

    private void handleClusterClientView( ClusterClientViewMessage msg )
    {
        myClusterClients = msg.clusterClients();
        buildTopology();
    }

    private Stream<ActorRef> myTopologyClients()
    {
        return myClusterClients
                .stream()
                .flatMap( readReplicaViewMessage::topologyClient );
    }

    private void sendTopologiesToClients( ReadReplicaViewActor.Tick ignored )
    {
        log.debug( "Sending to clients: %s, %s, %s", readReplicaTopology, coreTopology, databaseLeaderInfo );
        myTopologyClients().forEach( client -> {
            client.tell( readReplicaTopology, getSelf() );
            client.tell( coreTopology, getSelf() );
            client.tell( databaseLeaderInfo, getSelf() );
        } );
    }

    private void setCoreTopology( CoreTopology coreTopology )
    {
        this.coreTopology = coreTopology;
    }

    private void setDatabaseLeaderInfo( LeaderInfoDirectoryMessage leaderInfo )
    {
        this.databaseLeaderInfo = leaderInfo;
    }

    private void buildTopology()
    {
        log.debug( "Building read replica topology with read replicas: %s", readReplicaViewMessage );
        ReadReplicaTopology readReplicaTopology = readReplicaViewMessage.toReadReplicaTopology();
        log.debug( "Built read replica topology %s", readReplicaTopology );

        if ( !this.readReplicaTopology.equals( readReplicaTopology ) )
        {
            topologySink.offer( readReplicaTopology );
            this.readReplicaTopology = readReplicaTopology;
        }
    }
}
