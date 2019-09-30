/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.cluster.Cluster;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.cluster.ddata.LWWRegister;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.identity.ClusterId;

public class ClusterIdActor extends BaseReplicatedDataActor<LWWMap<String,ClusterId>>
{
    static final String CLUSTER_ID_PER_DB_KEY = "cluster-id-per-db-name";
    private final ActorRef coreTopologyActor;
    //We use a reverse clock because we want the ClusterId map to observe first-write-wins semantics, not the standard last-write-wins
    private final LWWRegister.Clock<ClusterId> clock = LWWRegister.reverseClock();
    private final int minRuntimeQuorumSize;

    ClusterIdActor( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, int minRuntimeCores )
    {
        super( cluster, replicator, LWWMapKey.create( CLUSTER_ID_PER_DB_KEY ), LWWMap::create );
        this.coreTopologyActor = coreTopologyActor;
        this.minRuntimeQuorumSize = ( minRuntimeCores / 2 ) + 1;
    }

    public static Props props( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, int minRuntimeCores )
    {
        return Props.create( ClusterIdActor.class, () -> new ClusterIdActor( cluster, replicator, coreTopologyActor, minRuntimeCores ) );
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no-op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( ClusterIdSetRequest.class,       this::setClusterId )
                .match( Replicator.UpdateSuccess.class, this::handleUpdateSuccess )
                .match( Replicator.GetSuccess.class,    this::validateClusterIdUpdate )
                .match( Replicator.UpdateTimeout.class, this::handleUpdateTimeout )
                .match( Replicator.UpdateFailure.class, this::handleUpdateFailure );
    }

    @Override
    protected void handleIncomingData( LWWMap<String,ClusterId> newData )
    {
        data = data.merge( newData );
        coreTopologyActor.tell( new ClusterIdDirectoryMessage( data ), getSelf() );
    }

    private void setClusterId( ClusterIdSetRequest message )
    {
        log().info( "Telling Replicator to set ClusterId to {}", message );
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
                .filter( m -> m instanceof ClusterIdSetRequest )
                .map( m -> (ClusterIdSetRequest) m )
                .ifPresent( m ->
                {
                    //Replicator.Update operations may return UpdateSuccess even if the merge function means that an update had
                    // no effect (i.e. there was a pre-existing clusterId). As a result, in the event of an UpdateSuccess response
                    // we must validate the impact by fetching the latest contents of the Replicator. We use a read quorum to
                    // ensure the update isn't validated against stale data.
                    Replicator.ReadConsistency readConsistency = new Replicator.ReadFrom( minRuntimeQuorumSize, m.timeout() );
                    Replicator.Get<LWWMap<String,ClusterId>> getOp = new Replicator.Get<>( key, readConsistency, Optional.of( m ) );
                    replicator.tell( getOp, getSelf() );
                } );
    }

    private void validateClusterIdUpdate( Replicator.GetSuccess<LWWMap<String,ClusterId>> getSuccess )
    {
        LWWMap<String,ClusterId> current = getSuccess.get( key );
        getSuccess.getRequest()
                .filter( m -> m instanceof ClusterIdSetRequest )
                .map( m -> (ClusterIdSetRequest) m )
                .ifPresent( m ->
                {
                    //The original ClusterIdSetRequest is passed through all messages in this actor as additional request context (.getRequest())
                    // we check that the original request was successful by checking whether the current cluster id for the given database is the
                    // same as that in the request.
                    ClusterId currentClusterId = current.getEntries().get( m.database() );
                    PublishClusterIdOutcome outcome = Objects.equals( currentClusterId, m.clusterId() ) ?
                                                      PublishClusterIdOutcome.SUCCESS :
                                                      PublishClusterIdOutcome.FAILURE;
                    m.replyTo().tell( outcome, getSelf() );
                } );
    }

    private void handleUpdateTimeout( Replicator.UpdateTimeout<?> updateTimeout )
    {
        updateTimeout.getRequest()
                .filter( m -> m instanceof ClusterIdSetRequest )
                .map( m -> (ClusterIdSetRequest) m )
                .ifPresent( m ->  m.replyTo().tell( PublishClusterIdOutcome.TIMEOUT, getSelf() ) );
    }

    private void handleUpdateFailure( Replicator.UpdateFailure<?> updateFailure )
    {
        updateFailure.getRequest()
                .filter( m -> m instanceof ClusterIdSetRequest )
                .map( m -> (ClusterIdSetRequest) m )
                .ifPresent( m ->
                {
                    String message = String.format( "Failed to set ClusterId with request %s. Failure was %s", m, updateFailure.toString() );
                    m.replyTo().tell( new Failure( new IllegalArgumentException( message ) ), getSelf() );
                } );
    }

    public enum PublishClusterIdOutcome
    {
        SUCCESS,
        FAILURE,
        TIMEOUT
    }
}
