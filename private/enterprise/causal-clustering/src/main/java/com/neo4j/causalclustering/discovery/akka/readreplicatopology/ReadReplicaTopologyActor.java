/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClientReceptionist;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.AllReplicatedDatabaseStates;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaViewActor.Tick;
import com.neo4j.configuration.CausalClusteringSettings;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toSet;

public class ReadReplicaTopologyActor extends AbstractLoggingActor
{
    private final SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink;
    private final SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink;

    private final Map<DatabaseId,DatabaseCoreTopology> coreTopologies = new HashMap<>();
    private final Map<DatabaseId,DatabaseReadReplicaTopology> readReplicaTopologies = new HashMap<>();
    private Map<DatabaseId,ReplicatedDatabaseState> coreMemberDbStates = new HashMap<>();
    private Map<DatabaseId,ReplicatedDatabaseState> rrMemberDbStates = new HashMap<>();
    private LeaderInfoDirectoryMessage databaseLeaderInfo = LeaderInfoDirectoryMessage.EMPTY;

    private Set<ActorRef> myClusterClients = new HashSet<>();
    private ReadReplicaViewMessage readReplicaView = ReadReplicaViewMessage.EMPTY;

    public static Props props( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
            SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink, ClusterClientReceptionist receptionist, Config config, Clock clock )
    {
        return Props.create( ReadReplicaTopologyActor.class,
                () -> new ReadReplicaTopologyActor( topologySink, databaseStateSink, receptionist, config, clock ) );
    }

    public static final String NAME = "cc-rr-topology-actor";

    ReadReplicaTopologyActor( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
            SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink, ClusterClientReceptionist receptionist, Config config, Clock clock )
    {
        this.topologySink = topologySink;
        this.databaseStateSink = databaseStateSink;

        Duration refresh = config.get( CausalClusteringSettings.cluster_topology_refresh );
        Props readReplicaViewProps = ReadReplicaViewActor.props( getSelf(), receptionist, clock, refresh );
        getContext().actorOf( readReplicaViewProps );

        Props clusterClientViewProps = ClusterClientViewActor.props( getSelf(), receptionist.underlying() );
        getContext().actorOf( clusterClientViewProps );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( AllReplicatedDatabaseStates.class,  this::updateCoreDatabaseStates )
                .match( ClusterClientViewMessage.class,     this::handleClusterClientView )
                .match( ReadReplicaViewMessage.class,       this::handleReadReplicaView )
                .match( Tick.class,      this::sendTopologiesToClients )
                .match( DatabaseCoreTopology.class,         this::addCoreTopology )
                .match( LeaderInfoDirectoryMessage.class,   this::setDatabaseLeaderInfo )
                .build();
    }

    private void updateCoreDatabaseStates( AllReplicatedDatabaseStates msg )
    {
        coreMemberDbStates = msg.databaseStates();
    }

    private void handleReadReplicaView( ReadReplicaViewMessage msg )
    {
        readReplicaView = msg;
        rrMemberDbStates = msg.allReadReplicaDatabaseStates();
        rrMemberDbStates.forEach( ( id, state ) -> databaseStateSink.offer( state ) );
        buildTopologies();
    }

    private void handleClusterClientView( ClusterClientViewMessage msg )
    {
        myClusterClients = msg.clusterClients();
        buildTopologies();
    }

    private Stream<ActorRef> knownTopologyClients()
    {
        return readReplicaView.topologyActorsForKnownClients( myClusterClients );
    }

    private void sendTopologiesToClients( Tick ignored )
    {
        log().debug( "Sending core topologies to clients: {}", coreTopologies );
        log().debug( "Sending read replica topologies to clients: {}", readReplicaTopologies );
        log().debug( "Sending database leader info to clients: {}", databaseLeaderInfo );
        log().debug( "Sending cores' database states to clients: {}", coreMemberDbStates );
        log().debug( "Sending read replicas' database states to clients: {}", rrMemberDbStates );

        knownTopologyClients().forEach( client -> {
            sendReadReplicaTopologiesTo( client );
            sendCoreTopologiesTo( client );
            client.tell( databaseLeaderInfo, getSelf() );
            sendDatabaseStatesTo( client );
        } );
    }

    private void sendReadReplicaTopologiesTo( ActorRef client )
    {
        for ( DatabaseReadReplicaTopology readReplicaTopology : readReplicaTopologies.values() )
        {
            client.tell( readReplicaTopology, getSelf() );
        }
    }

    private void sendCoreTopologiesTo( ActorRef client )
    {
        for ( DatabaseCoreTopology coreTopology : coreTopologies.values() )
        {
            client.tell( coreTopology, getSelf() );
        }
    }

    private void sendDatabaseStatesTo( ActorRef client )
    {
        for ( ReplicatedDatabaseState coreState : coreMemberDbStates.values() )
        {
            client.tell( coreState, getSelf() );
        }

        for ( ReplicatedDatabaseState replicaState : rrMemberDbStates.values() )
        {
            client.tell( replicaState, getSelf() );
        }
    }

    private void addCoreTopology( DatabaseCoreTopology coreTopology )
    {
        coreTopologies.put( coreTopology.databaseId(), coreTopology );
    }

    private void setDatabaseLeaderInfo( LeaderInfoDirectoryMessage leaderInfo )
    {
        this.databaseLeaderInfo = leaderInfo;
    }

    private void buildTopologies()
    {
        var receivedDatabaseIds = readReplicaView.databaseIds();

        var absentDatabaseIds = readReplicaTopologies.keySet()
                .stream()
                .filter( id -> !receivedDatabaseIds.contains( id ) )
                .collect( toSet() );

        // build empty topologies for database IDs cached locally but absent from the set of received database IDs
        absentDatabaseIds.forEach( this::buildTopology );

        // build topologies for the set of received database IDs
        receivedDatabaseIds.forEach( this::buildTopology );
    }

    private void buildTopology( DatabaseId databaseId )
    {
        log().debug( "Building read replica topology for database {} with read replicas: {}", databaseId, readReplicaView );
        DatabaseReadReplicaTopology readReplicaTopology = readReplicaView.toReadReplicaTopology( databaseId );
        log().debug( "Built read replica topology for database {}: {}", databaseId, readReplicaTopology );

        topologySink.offer( readReplicaTopology );
        if ( readReplicaTopology.servers().isEmpty() )
        {
            readReplicaTopologies.remove( databaseId );
        }
        else
        {
            readReplicaTopologies.put( databaseId, readReplicaTopology );
        }
    }
}
