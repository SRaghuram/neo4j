/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.client.ClusterClientUnreachable;
import akka.cluster.client.ClusterClientUp;
import akka.cluster.client.ClusterClients;
import akka.cluster.client.SubscribeClusterClients;
import akka.cluster.client.UnsubscribeClusterClients;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.ORSet;
import akka.cluster.ddata.ORSetKey;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.stream.Streams;

public class ReadReplicatorTopologyActor extends AbstractActor
{
    private static final Replicator.WriteConsistency METADATA_CONSISTENCY = new Replicator.WriteAll( new FiniteDuration( 10, TimeUnit.SECONDS ) );
    private final SourceQueueWithComplete<ReadReplicaTopology> topologySink;
    private final Cluster cluster;
    private final ActorRef replicator;
    private final ClusterClientReceptionist receptionist;
    private final Set<ActorRef> myClusterClients = new HashSet<>();
    private final Log log;
    private CoreTopology coreTopology = CoreTopology.EMPTY;
    private ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;

    // Cluster client / read replica metadata keys
    private final Key<LWWMap<ActorRef,ReadReplicaInfoMessage>> clientReadReplicaMapKey; // Stores client actor ref redundantly
    private  LWWMap<ActorRef,ReadReplicaInfoMessage> clusterClientReadReplicaMap;
    private final Key<ORSet<ActorRef>> reachableClientsKey;
    private ORSet<ActorRef> reachableClients;

    private final List<Key<?>> keys;
    private LeaderInfoDirectoryMessage databaseLeaderInfo = LeaderInfoDirectoryMessage.EMPTY;

    public static Props props( SourceQueueWithComplete<ReadReplicaTopology> topologySink, Cluster cluster, ActorRef replicator,
            ClusterClientReceptionist receptionist, LogProvider logProvider )
    {
        return Props.create( ReadReplicatorTopologyActor.class,
                () -> new ReadReplicatorTopologyActor( topologySink, cluster, replicator, receptionist, logProvider ) );
    }

    public static final String NAME = "cc-rr-topology-actor";
    static final String CLIENT_READ_REPLICA_MAP_KEY = "rr-member-lookup";
    static final String REACHABLE_CLIENTS_KEY = "rr-reachable-clients";

    ReadReplicatorTopologyActor( SourceQueueWithComplete<ReadReplicaTopology> topologySink, Cluster cluster, ActorRef replicator,
            ClusterClientReceptionist receptionist, LogProvider logProvider )
    {
        this.topologySink = topologySink;
        this.cluster = cluster;
        this.replicator = replicator;
        this.receptionist = receptionist;
        this.log = logProvider.getLog( getClass() );
        this.clientReadReplicaMapKey = LWWMapKey.create( CLIENT_READ_REPLICA_MAP_KEY );
        this.clusterClientReadReplicaMap = LWWMap.empty();
        this.reachableClientsKey = ORSetKey.create( REACHABLE_CLIENTS_KEY );
        this.reachableClients = ORSet.empty();
        this.keys = Arrays.asList( clientReadReplicaMapKey, reachableClientsKey );
    }

    @Override
    public void preStart()
    {
        keys.forEach( key -> replicator.tell( new Replicator.Subscribe<>( key, getSelf() ), ActorRef.noSender() ) );
        receptionist.registerService( getSelf() );
        receptionist.underlying().tell( SubscribeClusterClients.getInstance(), getSelf() );
    }

    @Override
    public void postStop()
    {
        keys.forEach( key -> replicator.tell( new Replicator.Unsubscribe<>( key, getSelf() ), ActorRef.noSender() ) );
        receptionist.unregisterService( getSelf() );
        receptionist.underlying().tell( UnsubscribeClusterClients.getInstance(), sender() );
    }

    @Override
    public Receive createReceive()
    {
        ReceiveBuilder receiveBuilder = ReceiveBuilder.create();
        matchClusterClientEvents( receiveBuilder );
        matchReadReplicaEvents( receiveBuilder );
        matchReplicatorEvents( receiveBuilder );
        matchApplicationEvents( receiveBuilder );
        return receiveBuilder.build();
    }

    private void matchClusterClientEvents( ReceiveBuilder builder )
    {
        builder
            .match( ClusterClients.class, msg -> {
                log.debug( "All cluster clients: %s", msg );
                myClusterClients.addAll( msg.getClusterClients() );
                ORSet<ActorRef> newReachable = msg.getClusterClients().stream()
                        .reduce( ORSet.create(),
                                ( set, client ) -> set.add( cluster, client ),
                                ORSet::merge );
                modifyReplicatedDataSet( reachableClientsKey, set -> set.merge( newReachable ) );
            } )
            .match( ClusterClientUp.class, msg -> {
                log.debug( "Cluster client up: %s", msg );
                myClusterClients.add( msg.clusterClient() );
                modifyReplicatedDataSet( reachableClientsKey, set -> set.add( cluster, msg.clusterClient() ) );
            } )
            .match( ClusterClientUnreachable.class, msg -> {
                log.debug( "Cluster client down: %s", msg );
                myClusterClients.remove( msg.clusterClient() );
                modifyReplicatedDataSet( reachableClientsKey, set -> set.remove( cluster, msg.clusterClient() ) );
            } );
    }

    private void matchReadReplicaEvents( ReceiveBuilder builder )
    {
         builder.match( ReadReplicaInfoMessage.class, msg -> {
            modifyReplicatedDataMap( clientReadReplicaMapKey, map -> map.put( cluster, msg.clusterClient(), msg ) );
             buildTopology();
             msg.topologyClientActorRef().tell( coreTopology, getSelf() );
             msg.topologyClientActorRef().tell( readReplicaTopology, getSelf() );
             msg.topologyClientActorRef().tell( databaseLeaderInfo, getSelf() );
         } ).match( ReadReplicaRemovalMessage.class, msg -> {
             log.debug( "Removing read replica info for %s", msg.clusterClient() );
             modifyReplicatedDataMap( clientReadReplicaMapKey, map -> map.remove( cluster, msg.clusterClient() ) );
         } );
    }

    @SuppressWarnings( "unchecked" )
    private void matchReplicatorEvents( ReceiveBuilder builder )
    {
        builder.match( Replicator.Changed.class, c -> c.key().equals( clientReadReplicaMapKey ), message ->
        {
            log.debug( "Incoming client-rr data %s", message );
            LWWMap<ActorRef,ReadReplicaInfoMessage> delta = (LWWMap<ActorRef,ReadReplicaInfoMessage>) message.dataValue();
            clusterClientReadReplicaMap = clusterClientReadReplicaMap.merge( delta );
            buildTopology();
        } ).match( Replicator.Changed.class, c -> c.key().equals( reachableClientsKey ), message ->
        {
            log.debug( "Incoming reachable client data %s", message );
            ORSet<ActorRef> delta = (ORSet<ActorRef>) message.dataValue();
            reachableClients = reachableClients.merge( delta );
            buildTopology();
        } ).match( Replicator.UpdateResponse.class, updated -> {
            log.debug( "Update: %s", updated );
        } ).match( Replicator.Changed.class, message -> log.info( "Unmatched replicator change event %s", message));
    }

    private void matchApplicationEvents( ReceiveBuilder builder )
    {
        builder.match( CoreTopology.class, topology -> {
            log.debug( "Sending coreTopology to read replicas" );
            this.coreTopology = topology;
            myTopologyClients().forEach( rrActorRef -> rrActorRef.tell( topology, getSelf() ) );
        } ).match( LeaderInfoDirectoryMessage.class, msg -> {
            log.debug( "Sending database leaders to read replicas" );
            this.databaseLeaderInfo = msg;
            myTopologyClients().forEach( client -> client.tell( msg, getSelf() ) );
        } );
    }

    private Stream<ActorRef> myTopologyClients()
    {
        return myClusterClients.stream()
                .flatMap( clusterClient -> Streams.ofNullable( clusterClientReadReplicaMap.getEntries().get( clusterClient ) ) )
                .map( ReadReplicaInfoMessage::topologyClientActorRef );
    }

    private <K,V> void modifyReplicatedDataMap( Key<LWWMap<K,V>> key, Function<LWWMap<K,V>,LWWMap<K,V>> modify )
    {
        modifyReplicatedData( key, modify, LWWMap::create );
    }

    private <T> void modifyReplicatedDataSet( Key<ORSet<T>> key, Function<ORSet<T>,ORSet<T>> modify )
    {
        modifyReplicatedData( key, modify, ORSet::create );
    }

    private  <T extends ReplicatedData> void modifyReplicatedData( Key<T> key, Function<T,T> modify, Supplier<T> empty )
    {
        Replicator.Update<T> update = new Replicator.Update<>( key, empty.get(), METADATA_CONSISTENCY, modify );

        replicator.tell( update, self() );
    }

    private void buildTopology()
    {
        log.debug( "Building read replica topology with clients: %s read replicas: %s", reachableClients, clusterClientReadReplicaMap );
        Map<MemberId,ReadReplicaInfo> knownReadReplicas = reachableClients
                .getElements()
                .stream()
                .flatMap( client -> Streams.ofNullable( clusterClientReadReplicaMap.getEntries().get( client ) ) )
                .map( rr -> Pair.of( rr.memberId(), rr.readReplicaInfo() ) )
                .collect( CollectorsUtil.pairsToMap() );

        ReadReplicaTopology readReplicaTopology = new ReadReplicaTopology( knownReadReplicas );
        log.debug( "Built read replica topology %s", readReplicaTopology );
        topologySink.offer( readReplicaTopology );
        this.readReplicaTopology = readReplicaTopology;
        myTopologyClients().forEach( ref -> ref.tell( readReplicaTopology, getSelf() ) );
    }
}
