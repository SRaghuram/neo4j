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
import com.neo4j.causalclustering.identity.ClusterId;

import org.neo4j.logging.LogProvider;

public class ClusterIdActor extends BaseReplicatedDataActor<LWWMap<String,ClusterId>>
{
    static final String CLUSTER_ID_PER_DB_KEY = "cluster-id-per-db-name";
    private final ActorRef coreTopologyActor;

    public ClusterIdActor( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, LogProvider logProvider )
    {
        super( cluster, replicator, LWWMapKey.create( CLUSTER_ID_PER_DB_KEY ), LWWMap::create, logProvider );
        this.coreTopologyActor = coreTopologyActor;
    }

    public static Props props( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, LogProvider logProvider )
    {
        return Props.create( ClusterIdActor.class, () -> new ClusterIdActor( cluster, replicator, coreTopologyActor, logProvider ) );
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
        builder.match( ClusterIdSettingMessage.class, message ->
                {
                    log.debug( "Setting ClusterId: %s", message );
                    modifyReplicatedData( key, map -> map.put( cluster, message.database(), message.clusterId() ) );
                } );
    }

    @Override
    protected void handleIncomingData( LWWMap<String,ClusterId> newData )
    {
        data = data.merge( newData );
        coreTopologyActor.tell( new ClusterIdDirectoryMessage( data ), getSelf() );
    }
}
