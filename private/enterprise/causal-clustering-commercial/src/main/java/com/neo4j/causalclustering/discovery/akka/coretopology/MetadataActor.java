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
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public class MetadataActor extends BaseReplicatedDataActor<LWWMap<UniqueAddress,CoreServerInfoForMemberId>>
{
    static Props props( MemberId myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, LogProvider logProvider )
    {
        return Props.create( MetadataActor.class, () -> new MetadataActor( myself, cluster, replicator, topologyActor, config, logProvider ) );
    }

    static final String MEMBER_DATA_KEY = "member-data";
    private final MemberId myself;

    private final ActorRef topologyActor;
    private final Config config;

    public MetadataActor( MemberId myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, LogProvider logProvider )
    {
        super( cluster, replicator, LWWMapKey.create( MEMBER_DATA_KEY ), LWWMap::empty, logProvider );
        this.myself = myself;
        this.topologyActor = topologyActor;
        this.config = config;
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( CleanupMessage.class, message -> removeDataFromReplicator( message.uniqueAddress() ) );
    }

    @Override
    public void sendInitialDataToReplicator()
    {
        CoreServerInfoForMemberId metadata = new CoreServerInfoForMemberId( myself, CoreServerInfo.from( config ) );
        modifyReplicatedData( key, map -> map.put( cluster, cluster.selfUniqueAddress(), metadata ) );
    }

    @Override
    public void removeDataFromReplicator( UniqueAddress uniqueAddress )
    {
        modifyReplicatedData( key, map -> map.remove( cluster, uniqueAddress ) );
    }

    @Override
    protected void handleIncomingData( LWWMap<UniqueAddress,CoreServerInfoForMemberId> delta )
    {
        data = data.merge( delta );
        topologyActor.tell( new MetadataMessage( data ), getSelf() );
    }
}
