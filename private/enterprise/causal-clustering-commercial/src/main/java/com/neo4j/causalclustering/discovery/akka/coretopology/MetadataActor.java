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

import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.METADATA;

public class MetadataActor extends BaseReplicatedDataActor<LWWMap<UniqueAddress,CoreServerInfoForMemberId>>
{
    static Props props( MemberId myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, ReplicatedDataMonitor monitor )
    {
        return Props.create( MetadataActor.class, () -> new MetadataActor( myself, cluster, replicator, topologyActor, config, monitor ) );
    }

    private final MemberId myself;

    private final ActorRef topologyActor;
    private final Config config;

    private MetadataActor( MemberId myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, ReplicatedDataMonitor monitor )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::empty, METADATA, monitor );
        this.myself = myself;
        this.topologyActor = topologyActor;
        this.config = config;
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( CleanupMessage.class, this::removeDataFromReplicator );
    }

    @Override
    public void sendInitialDataToReplicator()
    {
        CoreServerInfoForMemberId metadata = new CoreServerInfoForMemberId( myself, CoreServerInfo.from( config ) );

        log().info( "Telling Replicator to set CoreServerInfo for this address to {}", metadata );
        modifyReplicatedData( key, map -> map.put( cluster, cluster.selfUniqueAddress(), metadata ) );
    }

    private void removeDataFromReplicator( CleanupMessage message )
    {
        modifyReplicatedData( key, map -> map.remove( cluster, message.uniqueAddress() ) );
    }

    @Override
    protected void handleIncomingData( LWWMap<UniqueAddress,CoreServerInfoForMemberId> newData )
    {
        data = newData;
        topologyActor.tell( new MetadataMessage( data ), getSelf() );
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
