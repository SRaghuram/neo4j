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
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toSet;

public class ReadReplicaTopologyActor extends AbstractActor
{
    private final SourceQueueWithComplete<ReadReplicaTopology> topologySink;
    private final Log log;

    private final Map<DatabaseId,CoreTopology> coreTopologies = new HashMap<>();
    private final Map<DatabaseId,ReadReplicaTopology> readReplicaTopologies = new HashMap<>();
    private LeaderInfoDirectoryMessage databaseLeaderInfo = LeaderInfoDirectoryMessage.EMPTY;

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
                .match( CoreTopology.class,                 this::addCoreTopology )
                .match( LeaderInfoDirectoryMessage.class,   this::setDatabaseLeaderInfo )
                .build();
    }

    private void handleReadReplicaView( ReadReplicaViewMessage msg )
    {
        readReplicaViewMessage = msg;
        buildTopologies();
    }

    private void handleClusterClientView( ClusterClientViewMessage msg )
    {
        myClusterClients = msg.clusterClients();
        buildTopologies();
    }

    private Stream<ActorRef> myTopologyClients()
    {
        return myClusterClients
                .stream()
                .flatMap( readReplicaViewMessage::topologyClient );
    }

    private void sendTopologiesToClients( ReadReplicaViewActor.Tick ignored )
    {
        log.debug( "Sending to clients: %s, %s, %s", readReplicaTopologies, coreTopologies, databaseLeaderInfo );
        myTopologyClients().forEach( client -> {
            sendReadReplicaTopologiesTo( client );
            sendCoreTopologiesTo( client );
            client.tell( databaseLeaderInfo, getSelf() );
        } );
    }

    private void sendReadReplicaTopologiesTo( ActorRef client )
    {
        for ( ReadReplicaTopology readReplicaTopology : readReplicaTopologies.values() )
        {
            client.tell( readReplicaTopology, getSelf() );
        }
    }

    private void sendCoreTopologiesTo( ActorRef client )
    {
        for ( CoreTopology coreTopology : coreTopologies.values() )
        {
            client.tell( coreTopology, getSelf() );
        }
    }

    private void addCoreTopology( CoreTopology coreTopology )
    {
        coreTopologies.put( coreTopology.databaseId(), coreTopology );
    }

    private void setDatabaseLeaderInfo( LeaderInfoDirectoryMessage leaderInfo )
    {
        this.databaseLeaderInfo = leaderInfo;
    }

    private void buildTopologies()
    {
        var receivedDatabaseIds = readReplicaViewMessage.databaseIds();

        // build topologies for the set of received database IDs
        receivedDatabaseIds.forEach( this::buildTopology );

        // build empty topologies for database IDs cached locally but absent from the set of received database IDs
        var absentDatabaseIds = readReplicaTopologies.keySet()
                .stream()
                .filter( id -> !receivedDatabaseIds.contains( id ) )
                .collect( toSet() );

        absentDatabaseIds.forEach( this::buildTopology );
    }

    private void buildTopology( DatabaseId databaseId )
    {
        log.debug( "Building read replica topology for database %s with read replicas: %s", databaseId.name(), readReplicaViewMessage );
        ReadReplicaTopology readReplicaTopology = readReplicaViewMessage.toReadReplicaTopology( databaseId );
        log.debug( "Built read replica topology for database %s: %s", databaseId.name(), readReplicaTopology );

        topologySink.offer( readReplicaTopology );
        if ( readReplicaTopology.members().isEmpty() )
        {
            readReplicaTopologies.remove( databaseId );
        }
        else
        {
            readReplicaTopologies.put( databaseId, readReplicaTopology );
        }
    }
}
