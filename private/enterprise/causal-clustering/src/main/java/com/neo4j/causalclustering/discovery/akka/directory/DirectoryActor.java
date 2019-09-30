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
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseId;

public class DirectoryActor extends BaseReplicatedDataActor<ORMap<DatabaseId,ReplicatedLeaderInfo>>
{
    public static Props props( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoveryUpdateSink,
            ActorRef rrTopologyActor )
    {
        return Props.create( DirectoryActor.class, () -> new DirectoryActor( cluster, replicator, discoveryUpdateSink, rrTopologyActor ) );
    }

    static final String PER_DB_LEADER_KEY = "per-db-leader-name";
    public static final String NAME = "cc-directory-actor";

    private final SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoveryUpdateSink;
    private final ActorRef rrTopologyActor;

    private DirectoryActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoveryUpdateSink,
            ActorRef rrTopologyActor )
    {
        super( cluster, replicator, ORMapKey.create( PER_DB_LEADER_KEY ), ORMap::create );
        this.discoveryUpdateSink = discoveryUpdateSink;
        this.rrTopologyActor = rrTopologyActor;
    }

    @Override
    protected void sendInitialDataToReplicator()
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
    protected void handleIncomingData( ORMap<DatabaseId,ReplicatedLeaderInfo> newData )
    {
        data = data.merge( newData );
        Map<DatabaseId,LeaderInfo> leaderInfos = data.getEntries().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().leaderInfo() ) );
        discoveryUpdateSink.offer( leaderInfos );
        rrTopologyActor.tell( new LeaderInfoDirectoryMessage( leaderInfos ), getSelf() );
    }
}
