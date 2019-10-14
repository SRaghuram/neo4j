/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.identity.RaftId;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.RAFT_ID;

public class RaftIdActor extends BaseReplicatedDataActor<LWWMap<DatabaseId,RaftId>>
{
    private final ActorRef coreTopologyActor;

    public RaftIdActor( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, ReplicatedDataMonitor monitors )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::create, RAFT_ID, monitors );
        this.coreTopologyActor = coreTopologyActor;
    }

    public static Props props( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, ReplicatedDataMonitor monitors )
    {
        return Props.create( RaftIdActor.class, () -> new RaftIdActor( cluster, replicator, coreTopologyActor, monitors ) );
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no-op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( RaftIdSettingMessage.class, message ->
                {
                    log().debug( "Setting RaftId: {}", message );
                    modifyReplicatedData( key, map -> map.put( cluster, message.database(), message.raftId() ) );
                } );
    }

    @Override
    protected void handleIncomingData( LWWMap<DatabaseId,RaftId> newData )
    {
        data = newData;
        coreTopologyActor.tell( new RaftIdDirectoryMessage( data ), getSelf() );
    }

    @Override
    protected int dataMetricVisible()
    {
        return data.size();
    }

    @Override
    protected int dataMetricInvisible()
    {
        return data.underlying().keys().vvector().size();
    }
}
