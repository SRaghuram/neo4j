/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.METADATA;

public class MetadataActor extends BaseReplicatedDataActor<LWWMap<UniqueAddress,CoreServerInfoForServerId>>
{
    static Props props( Cluster cluster, ActorRef replicator, ActorRef topologyActor, ActorRef raftMappingActor,
            Config config, ReplicatedDataMonitor monitor, ServerId myself )
    {
        return Props.create( MetadataActor.class, () -> new MetadataActor( cluster, replicator, topologyActor, raftMappingActor,
                                                                           config, monitor, myself ) );
    }

    private final ActorRef topologyActor;
    private final ActorRef raftMappingActor;
    private final Config config;
    private final ServerId myself;
    private final Set<DatabaseId> startedDatabases = new HashSet<>();

    private MetadataActor( Cluster cluster, ActorRef replicator, ActorRef topologyActor, ActorRef raftMappingActor,
            Config config, ReplicatedDataMonitor monitor, ServerId myself )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::empty, METADATA, monitor );
        this.topologyActor = topologyActor;
        this.raftMappingActor = raftMappingActor;
        this.config = config;
        this.myself = myself;
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( CleanupMessage.class,            this::removeDataFromReplicator )
                .match( DatabaseStartedMessage.class,   this::handleDatabaseStartedMessage )
                .match( DatabaseStoppedMessage.class,   this::handleDatabaseStoppedMessage );
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        if ( startedDatabases.add( message.namedDatabaseId().databaseId() ) )
        {
            sendCoreServerInfo();
        }
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        if ( startedDatabases.remove( message.namedDatabaseId().databaseId() ) )
        {
            sendCoreServerInfo();
        }
    }

    @Override
    public void sendInitialDataToReplicator( ServerSnapshot serverSnapshot )
    {
        var databaseIds = serverSnapshot.discoverableDatabases();
        startedDatabases.addAll( databaseIds );
        sendCoreServerInfo();
    }

    private void removeDataFromReplicator( CleanupMessage message )
    {
        var coreServerInfo = data.getEntries().get( message.uniqueAddress() );
        modifyReplicatedData( key, map -> map.remove( cluster, message.uniqueAddress() ) );
        if ( coreServerInfo != null )
        {
            raftMappingActor.tell( new RaftMemberMappingActor.CleanupMessage( coreServerInfo.serverId() ), getSelf() );
        }
    }

    @Override
    protected void handleIncomingData( LWWMap<UniqueAddress,CoreServerInfoForServerId> newData )
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

    private void sendCoreServerInfo()
    {
        var startedDatabases = Set.copyOf( this.startedDatabases );
        var info = CoreServerInfo.from( config, startedDatabases );
        var metadata = new CoreServerInfoForServerId( myself, info );
        modifyReplicatedData( key, map -> map.put( cluster, cluster.selfUniqueAddress(), metadata ) );
    }
}
