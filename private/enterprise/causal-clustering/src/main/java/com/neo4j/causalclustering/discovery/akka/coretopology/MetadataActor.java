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
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;

public class MetadataActor extends BaseReplicatedDataActor<LWWMap<UniqueAddress,CoreServerInfoForMemberId>>
{
    static Props props( DiscoveryMember myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config )
    {
        return Props.create( MetadataActor.class, () -> new MetadataActor( myself, cluster, replicator, topologyActor, config ) );
    }

    static final String MEMBER_DATA_KEY = "member-data";

    private final DiscoveryMember myself;
    private final ActorRef topologyActor;
    private final Config config;

    private final Set<DatabaseId> startedDatabases = new HashSet<>();

    private MetadataActor( DiscoveryMember myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config )
    {
        super( cluster, replicator, LWWMapKey.create( MEMBER_DATA_KEY ), LWWMap::empty );
        this.myself = myself;
        this.topologyActor = topologyActor;
        this.config = config;
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder .match( CleanupMessage.class,           this::removeDataFromReplicator )
                .match( DatabaseStartedMessage.class,   this::handleDatabaseStartedMessage )
                .match( DatabaseStoppedMessage.class,   this::handleDatabaseStoppedMessage );
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        if ( startedDatabases.add( message.databaseId() ) )
        {
            sendCoreServerInfo();
        }
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        if ( startedDatabases.remove( message.databaseId() ) )
        {
            sendCoreServerInfo();
        }
    }

    @Override
    public void sendInitialDataToReplicator()
    {
        startedDatabases.addAll( myself.startedDatabases() );
        sendCoreServerInfo();
    }

    private void removeDataFromReplicator( CleanupMessage message )
    {
        modifyReplicatedData( key, map -> map.remove( cluster, message.uniqueAddress() ) );
    }

    @Override
    protected void handleIncomingData( LWWMap<UniqueAddress,CoreServerInfoForMemberId> delta )
    {
        data = data.merge( delta );
        topologyActor.tell( new MetadataMessage( data ), getSelf() );
    }

    private void sendCoreServerInfo()
    {
        var databaseIds = Set.copyOf( startedDatabases );
        var info = CoreServerInfo.from( config, databaseIds );
        var metadata = new CoreServerInfoForMemberId( myself.id(), info );
        modifyReplicatedData( key, map -> map.put( cluster, cluster.selfUniqueAddress(), metadata ) );
    }
}
