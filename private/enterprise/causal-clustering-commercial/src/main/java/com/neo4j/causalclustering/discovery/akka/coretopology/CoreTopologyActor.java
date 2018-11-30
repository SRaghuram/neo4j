/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.stream.javadsl.SourceQueueWithComplete;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CoreTopologyActor extends AbstractActorWithTimers
{
    public static Props props( MemberId myself, SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink, ActorRef rrTopologyActor, ActorRef replicator,
            Cluster cluster, TopologyBuilder topologyBuilder, Config config, LogProvider logProvider )
    {
        return Props.create( CoreTopologyActor.class,
                () -> new CoreTopologyActor( myself, topologyUpdateSink, rrTopologyActor, replicator, cluster, topologyBuilder, config, logProvider ) );
    }

    public static final String NAME = "cc-core-topology-actor";

    private final SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink;
    private final TopologyBuilder topologyBuilder;
    private final String databaseName;

    private final Address myAddress;

    private final Log log;

    private final ActorRef clusterIdActor;
    private final ActorRef readReplicaTopologyActor;

    // Topology component data
    private MetadataMessage memberData;
    private ClusterIdDirectoryMessage clusterIdPerDb;
    private ClusterViewMessage clusterView;

    private CoreTopology coreTopology;

    CoreTopologyActor( MemberId myself,
            SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink,
            ActorRef readReplicaTopologyActor,
            ActorRef replicator,
            Cluster cluster,
            TopologyBuilder topologyBuilder,
            Config config,
            LogProvider logProvider )
    {
        this.topologyUpdateSink = topologyUpdateSink;
        this.readReplicaTopologyActor = readReplicaTopologyActor;
        this.topologyBuilder = topologyBuilder;
        this.memberData = MetadataMessage.EMPTY;
        this.clusterIdPerDb = ClusterIdDirectoryMessage.EMPTY;
        this.databaseName = config.get( CausalClusteringSettings.database );
        this.log = logProvider.getLog( getClass() );
        this.clusterView = ClusterViewMessage.EMPTY;
        this.coreTopology = CoreTopology.EMPTY;
        this.myAddress = cluster.selfAddress();

        // Children, who will be sending messages to us
        ActorRef metadataActor = getContext().actorOf( MetadataActor.props( myself, cluster, replicator, getSelf(), config, logProvider ) );
        ActorRef downingActor = getContext().actorOf( ClusterDowningActor.props( cluster, metadataActor, logProvider ) );
        getContext().actorOf( ClusterStateActor.props( cluster, getSelf(), downingActor, config, logProvider ) );
        clusterIdActor = getContext().actorOf( ClusterIdActor.props( cluster, replicator, getSelf(), logProvider ) );
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match( ClusterViewMessage.class,        this::handleClusterViewMessage)
                .match( MetadataMessage.class,           this::handleMetadataMessage )
                .match( ClusterIdDirectoryMessage.class, this::handleClusterIdDirectoryMessage )
                .match( ClusterIdSettingMessage.class,   this::handleClusterIdSettingMessage )
                .build();
    }

    private void handleClusterViewMessage( ClusterViewMessage message )
    {
        clusterView = message;
        buildTopology();
    }

    private void handleMetadataMessage( MetadataMessage message )
    {
        memberData = message;
        buildTopology();
    }

    private void handleClusterIdDirectoryMessage( ClusterIdDirectoryMessage message )
    {
        clusterIdPerDb = message;
        buildTopology();
    }

    private void handleClusterIdSettingMessage( ClusterIdSettingMessage message )
    {
        clusterIdActor.forward( message, context() );
    }

    private void buildTopology()
    {
        CoreTopology newCoreTopology = topologyBuilder.buildCoreTopology( clusterIdPerDb.get( databaseName ), clusterView, memberData );
        if ( !this.coreTopology.equals( newCoreTopology ) || !Objects.equals( this.coreTopology.clusterId(),  newCoreTopology.clusterId() ) )
        {
            this.coreTopology = newCoreTopology;
            Collection<Address> akkaMemberAddresses = clusterView.members()
                    .stream()
                    .map( Member::address )
                    .filter( addr -> !addr.equals( myAddress ) )
                    .collect( Collectors.toList() );
            topologyUpdateSink.offer( new CoreTopologyMessage( newCoreTopology, akkaMemberAddresses ) );
            readReplicaTopologyActor.tell( newCoreTopology, getSelf() );
        }
    }
}
