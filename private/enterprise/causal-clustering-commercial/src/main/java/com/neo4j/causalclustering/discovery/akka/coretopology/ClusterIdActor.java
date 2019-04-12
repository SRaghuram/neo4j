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
import akka.cluster.ddata.LWWRegister;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;

import java.util.Optional;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.logging.LogProvider;

public class ClusterIdActor extends BaseReplicatedDataActor<LWWMap<String,ClusterId>>
{
    static final String CLUSTER_ID_PER_DB_KEY = "cluster-id-per-db-name";
    private final ActorRef coreTopologyActor;
    //We use a reverse clock because we want the ClusterId map to observe first-write-wins semantics, not the standard last-write-wins
    private final LWWRegister.Clock<ClusterId> clock = LWWRegister.reverseClock();

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
        builder.match( ClusterIdSettingMessage.class, this::setClusterId )
                .match( Replicator.UpdateSuccess.class, this::handleUpdateSuccess )
                .match( Replicator.GetSuccess.class, this::replyToClusterIdSetter )
                .match( Replicator.UpdateFailure.class, this::handleUpdateFailure );

    }

    @Override
    protected void handleIncomingData( LWWMap<String,ClusterId> newData )
    {
        data = data.merge( newData );
        coreTopologyActor.tell( new ClusterIdDirectoryMessage( data ), getSelf() );
    }

    private void setClusterId( ClusterIdSettingMessage message )
    {
        log.debug( "Setting ClusterId: %s", message );
        modifyReplicatedData( key, map ->
        {
            if ( map.contains( message.database() ) )
            {
                return map;
            }
            return map.put( cluster, message.database(), message.clusterId(), clock );
        }, message.withReplyTo( getSender() ) );
    }

    private void handleUpdateSuccess( Replicator.UpdateSuccess<?> updateSuccess )
    {
        updateSuccess.getRequest()
                .filter( m -> m instanceof ClusterIdSettingMessage )
                .map( m -> (ClusterIdSettingMessage) m )
                .ifPresent( m ->
                {
                    Replicator.Get<LWWMap<String,ClusterId>> getOp = new Replicator.Get<>( key, READ_CONSISTENCY, Optional.of( m ) );
                    replicator.tell( getOp, getSelf() );
                } );
    }

    private void replyToClusterIdSetter( Replicator.GetSuccess<LWWMap<String,ClusterId>> getSuccess )
    {
        LWWMap<String,ClusterId> latest = getSuccess.get( key );
        getSuccess.getRequest()
                .filter( m -> m instanceof ClusterIdSettingMessage )
                .map( m -> (ClusterIdSettingMessage) m )
                .ifPresent( m ->
                {
                    ClusterIdSettingMessage response = new ClusterIdSettingMessage( latest.getEntries().get( m.database() ), m.database() );
                    m.replyTo().tell( response, getSelf() );
                } );
    }

    private void handleUpdateFailure( Replicator.UpdateFailure<?> updateFailure )
    {
        updateFailure.getRequest()
                .filter( m -> m instanceof ClusterIdSettingMessage )
                .map( m -> (ClusterIdSettingMessage) m )
                .ifPresent( m ->
                {
                    String message = String.format( "Failed to set ClusterId: %s", m );
                    log.warn( message );
                    m.replyTo().tell( new akka.actor.Status.Failure( new IllegalArgumentException( message ) ), getSelf() );
                } );
    }
}
