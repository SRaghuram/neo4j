/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.RAFT_MEMBER_MAPPING;

/**
 * Keeps track of raft member IDs for every database/server pair.
 */
public class RaftMemberMappingActor extends BaseReplicatedDataActor<LWWMap<DatabaseServer,RaftMemberId>>
{
    static Props props( Cluster cluster, ActorRef replicator, ActorRef topologyActor, CoreServerIdentity myIdentity,
            ReplicatedDataMonitor monitor )
    {
        return Props.create(
                RaftMemberMappingActor.class, () -> new RaftMemberMappingActor( cluster, replicator, topologyActor, myIdentity, monitor ) );
    }

    private final ActorRef topologyActor;
    private final CoreServerIdentity myIdentity;

    private RaftMemberMappingActor( Cluster cluster, ActorRef replicator, ActorRef topologyActor, CoreServerIdentity myIdentity,
            ReplicatedDataMonitor monitor )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::empty, RAFT_MEMBER_MAPPING, monitor );
        this.topologyActor = topologyActor;
        this.myIdentity = myIdentity;
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( CleanupMessage.class,         this::removeDataFromReplicator )
               .match( RaftMemberKnownMessage.class, this::handleRaftMemberKnownMessage )
               .match( DatabaseStoppedMessage.class, this::handleDatabaseStoppedMessage );
    }

    private void handleRaftMemberKnownMessage( RaftMemberKnownMessage message )
    {
        var raftMemberId = myIdentity.raftMemberId( message.namedDatabaseId() );
        var mapping = new DatabaseServer( message.namedDatabaseId().databaseId(), myIdentity.serverId() );
        log().info( "add mapping {}", mapping );
        modifyReplicatedData( key, map -> map.put( cluster, mapping, raftMemberId ) );
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        var mapping = new DatabaseServer( message.namedDatabaseId().databaseId(), myIdentity.serverId() );
        log().info( "remove mapping {}", mapping );
        modifyReplicatedData( key, map -> map.remove( cluster, mapping ) );
    }

    @Override
    public void sendInitialDataToReplicator( ServerSnapshot serverSnapshot )
    {
        var serverId = myIdentity.serverId();
        var localMappings = serverSnapshot.discoverableDatabases().stream()
                                          .map( databaseId -> new DatabaseServer( databaseId, serverId ) )
                                          .reduce( LWWMap.create(), this::addMapping, LWWMap::merge );

        log().info( "add initial mappings {}", localMappings );
        modifyReplicatedData( key, map -> map.merge( localMappings ) );
    }

    private LWWMap<DatabaseServer,RaftMemberId> addMapping( LWWMap<DatabaseServer,RaftMemberId> acc,
            DatabaseServer key )
    {
        return acc.put( cluster, key, myIdentity.raftMemberId( key.databaseId() ) );
    }

    private void removeDataFromReplicator( CleanupMessage message )
    {
        data.getEntries().keySet().stream()
                .filter( ds -> ds.serverId().equals( message.serverId ) )
                .peek( mapping -> log().info( "remove mapping {}", mapping ) )
                .forEach( ds -> modifyReplicatedData( key, map -> map.remove( cluster, ds ) ));
    }

    @Override
    protected void handleIncomingData( LWWMap<DatabaseServer,RaftMemberId> newData )
    {
        log().info( "incoming mappings {}", newData );
        data = newData;
        topologyActor.tell( new RaftMemberMappingMessage( data ), getSelf() );
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

    static class CleanupMessage
    {
        private final ServerId serverId;

        CleanupMessage( ServerId serverId )
        {
            this.serverId = serverId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            CleanupMessage that = (CleanupMessage) o;
            return Objects.equals( serverId, that.serverId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( serverId );
        }
    }
}
