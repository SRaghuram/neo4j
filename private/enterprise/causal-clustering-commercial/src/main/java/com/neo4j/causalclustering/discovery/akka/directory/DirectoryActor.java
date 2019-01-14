/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.ORMap;
import akka.cluster.ddata.ORMapKey;
import akka.cluster.UniqueAddress;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.logging.LogProvider;

public class DirectoryActor extends BaseReplicatedDataActor<ORMap<String,ReplicatedLeaderInfo>>
{
    public static Props props( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink,
            ActorRef rrTopologyActor, LogProvider logProvider )
    {
        return Props.create( DirectoryActor.class, () -> new DirectoryActor( cluster, replicator, discoveryUpdateSink, rrTopologyActor, logProvider ) );
    }

    static final String PER_DB_LEADER_KEY = "per-db-leader-name";
    public static final String NAME = "cc-directory-actor";

    private final SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink;
    private final ActorRef rrTopologyActor;

    protected DirectoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<String,LeaderInfo>> discoveryUpdateSink,
            ActorRef rrTopologyActor, LogProvider logProvider )
    {
        super( cluster, replicator, ORMapKey.create( PER_DB_LEADER_KEY ), ORMap::create, logProvider );
        this.discoveryUpdateSink = discoveryUpdateSink;
        this.rrTopologyActor = rrTopologyActor;
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no op
    }

    @Override
    protected void removeDataFromReplicator( UniqueAddress uniqueAddress )
    {
        // no op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( LeaderInfoSettingMessage.class, message ->
            modifyReplicatedData( key, map -> map.put( cluster, message.database(), new ReplicatedLeaderInfo( message.leaderInfo() ) ) ) );
    }

    @Override
    protected void handleIncomingData( ORMap<String,ReplicatedLeaderInfo> newData )
    {
        data = data.merge( newData );
        Map<String,LeaderInfo> leaderInfos = data.getEntries().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().leaderInfo() ) );
        discoveryUpdateSink.offer( leaderInfos );
        rrTopologyActor.tell( new LeaderInfoDirectoryMessage( leaderInfos ), getSelf() );
    }
}
