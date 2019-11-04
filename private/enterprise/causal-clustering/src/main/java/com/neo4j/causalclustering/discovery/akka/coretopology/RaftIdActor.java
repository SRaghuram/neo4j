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
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.RAFT_ID;

public class RaftIdActor extends BaseReplicatedDataActor<LWWMap<RaftId,MemberId>>
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
                    modifyReplicatedData( key, map ->
                    {
                        if ( map.contains( message.raftId() ) )
                        {
                            return map;
                        }
                        return map.put( cluster, message.raftId(), message.publisher() );
                    } );
                } );
    }

    @Override
    protected void handleIncomingData( LWWMap<RaftId,MemberId> newData )
    {
        data = newData;
        coreTopologyActor.tell( new BootstrappedRaftsMessage( data.getEntries().keySet() ), getSelf() );
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
