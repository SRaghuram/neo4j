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
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.METADATA;

public class MetadataActor extends BaseReplicatedDataActor<LWWMap<UniqueAddress,CoreServerInfoForServerId>>
{
    static Props props( DiscoveryMember myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, ReplicatedDataMonitor monitor )
    {
        return Props.create( MetadataActor.class, () -> new MetadataActor( myself, cluster, replicator, topologyActor, config, monitor ) );
    }

    private final DiscoveryMember myself;
    private final ActorRef topologyActor;
    private final Config config;

    private final Set<DatabaseId> startedDatabases = new HashSet<>();

    private MetadataActor( DiscoveryMember myself, Cluster cluster, ActorRef replicator, ActorRef topologyActor, Config config, ReplicatedDataMonitor monitor )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::empty, METADATA, monitor );
        this.myself = myself;
        this.topologyActor = topologyActor;
        this.config = config;
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
    public void sendInitialDataToReplicator()
    {
        var databaseIds = myself.startedDatabases().stream()
                                  .map( NamedDatabaseId::databaseId )
                                  .collect( Collectors.toSet() );
        startedDatabases.addAll( databaseIds );
        sendCoreServerInfo();
    }

    private void removeDataFromReplicator( CleanupMessage message )
    {
        modifyReplicatedData( key, map -> map.remove( cluster, message.uniqueAddress() ) );
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
        var metadata = new CoreServerInfoForServerId( myself.id(), info );
        modifyReplicatedData( key, map -> map.put( cluster, cluster.selfUniqueAddress(), metadata ) );
    }
}
