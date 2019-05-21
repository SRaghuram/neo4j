/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.UniqueAddress;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import com.neo4j.causalclustering.identity.RaftId;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

public class RaftIdActor extends BaseReplicatedDataActor<LWWMap<DatabaseId,RaftId>>
{
    static final String RAFT_ID_PER_DB_KEY = "raft-id-per-db-name";
    private final ActorRef coreTopologyActor;

    public RaftIdActor( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, LogProvider logProvider )
    {
        super( cluster, replicator, LWWMapKey.create( RAFT_ID_PER_DB_KEY ), LWWMap::create, logProvider );
        this.coreTopologyActor = coreTopologyActor;
    }

    public static Props props( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, LogProvider logProvider )
    {
        return Props.create( RaftIdActor.class, () -> new RaftIdActor( cluster, replicator, coreTopologyActor, logProvider ) );
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no-op
    }

    @Override
    protected void removeDataFromReplicator( UniqueAddress uniqueAddress )
    {
        // no-op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( RaftIdSettingMessage.class, message ->
                {
                    log.debug( "Setting RaftId: %s", message );
                    modifyReplicatedData( key, map -> map.put( cluster, message.database(), message.raftId() ) );
                } );
    }

    @Override
    protected void handleIncomingData( LWWMap<DatabaseId,RaftId> newData )
    {
        data = data.merge( newData );
        coreTopologyActor.tell( new RaftIdDirectoryMessage( data ), getSelf() );
    }
}
