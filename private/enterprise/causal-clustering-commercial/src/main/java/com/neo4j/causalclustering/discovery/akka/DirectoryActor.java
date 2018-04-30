/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;

import java.util.Map;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.logging.LogProvider;

public class DirectoryActor extends BaseReplicatedDataActor<LWWMap<String,LeaderInfo>>
{
    static Props props( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink, ActorRef rrTopologyActor,
            LogProvider logProvider )
    {
        return Props.create( DirectoryActor.class, () -> new DirectoryActor( cluster, replicator, discoveryUpdateSink, rrTopologyActor, logProvider ) );
    }

    static final String PER_DB_LEADER_KEY = "per-db-leader-name";
    static final String NAME = "cc-directory-actor";

    private final SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink;
    private final ActorRef rrTopologyActor;

    protected DirectoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink,
            ActorRef rrTopologyActor, LogProvider logProvider )
    {
        super( cluster, replicator, LWWMapKey.create( PER_DB_LEADER_KEY ), LWWMap::create, logProvider );
        this.discoveryUpdateSink = discoveryUpdateSink;
        this.rrTopologyActor = rrTopologyActor;
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no op
    }

    @Override
    protected void removeDataFromReplicator()
    {
        // no op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( LeaderInfoForDatabase.class, message -> {
            modifyReplicatedData( key, map -> map.put( cluster, message.database(), message.leaderInfo() ) );
        } );
    }

    @Override
    protected void handleIncomingData( LWWMap<String,LeaderInfo> newData )
    {
        data = data.merge( newData );
        discoveryUpdateSink.offer( data.getEntries() );
        rrTopologyActor.tell( new DatabaseLeaderInfoMessage( data.getEntries() ), getSelf() );
    }
}
