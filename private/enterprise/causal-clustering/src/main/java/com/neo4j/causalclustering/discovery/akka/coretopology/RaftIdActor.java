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
import akka.cluster.ddata.LWWRegister;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.PublishRaftIdOutcome;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Objects;
import java.util.Optional;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.RAFT_ID;

public class RaftIdActor extends BaseReplicatedDataActor<LWWMap<RaftId,MemberId>>
{
    private final ActorRef coreTopologyActor;
    //We use a reverse clock because we want the RaftId map to observe first-write-wins semantics, not the standard last-write-wins
    private final LWWRegister.Clock<RaftId> clock = LWWRegister.reverseClock();
    private final int minRuntimeQuorumSize;

    RaftIdActor( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, ReplicatedDataMonitor monitors, int minRuntimeCores )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::create, RAFT_ID, monitors );
        this.coreTopologyActor = coreTopologyActor;
        this.minRuntimeQuorumSize = ( minRuntimeCores / 2 ) + 1;
    }

    public static Props props( Cluster cluster, ActorRef replicator, ActorRef coreTopologyActor, ReplicatedDataMonitor monitors, int minRuntimeCores )
    {
        return Props.create( RaftIdActor.class, () -> new RaftIdActor( cluster, replicator, coreTopologyActor, monitors, minRuntimeCores ) );
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no-op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( RaftIdSetRequest.class,          this::setRaftId )
                .match( Replicator.UpdateSuccess.class, this::handleUpdateSuccess )
                .match( Replicator.GetSuccess.class,    this::validateRaftIdUpdate )
                .match( Replicator.UpdateFailure.class, this::handleUpdateFailure );
    }

    private void setRaftId( RaftIdSetRequest message )
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
    }

    private void handleUpdateSuccess( Replicator.UpdateSuccess<?> updateSuccess )
    {
        updateSuccess.getRequest()
                .filter( m -> m instanceof RaftIdSetRequest )
                .map( m -> (RaftIdSetRequest) m )
                .ifPresent( m ->
                {
                    //Replicator.Update operations may return UpdateSuccess even if the merge function means that an update had
                    // no effect (i.e. there was a pre-existing clusterId). As a result, in the event of an UpdateSuccess response
                    // we must validate the impact by fetching the latest contents of the Replicator. We use a read quorum to
                    // ensure the update isn't validated against stale data.
                    Replicator.ReadConsistency readConsistency = new Replicator.ReadFrom( minRuntimeQuorumSize, m.timeout() );
                    Replicator.Get<LWWMap<RaftId,MemberId>> getOp = new Replicator.Get<>( key, readConsistency, Optional.of( m ) );
                    replicator.tell( getOp, getSelf() );
                } );
    }

    private void validateRaftIdUpdate( Replicator.GetSuccess<LWWMap<RaftId,MemberId>> getSuccess )
    {
        LWWMap<RaftId,MemberId> current = getSuccess.get( key );
        getSuccess.getRequest()
                .filter( m -> m instanceof RaftIdSetRequest )
                .map( m -> (RaftIdSetRequest) m )
                .ifPresent( request ->
                {
                    //The original RaftIdSetRequest is passed through all messages in this actor as additional request context (.getRequest())
                    // we check whether the request sent by this actor was successful by checking whether the publisher MemberId for the RaftId
                    // stored in the replicator is the same as that in the request. If the two MemberIds do not match, then another member
                    // succeeded in publishing earlier than us and we should fail.
                    MemberId successfulPublisher = current.getEntries().get( request.raftId() );
                    PublishRaftIdOutcome outcome;
                    if ( successfulPublisher == null )
                    {
                        outcome = PublishRaftIdOutcome.FAILED_PUBLISH;
                    }
                    else if ( Objects.equals( successfulPublisher, request.publisher() ) )
                    {
                        outcome = PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_ME;
                    }
                    else
                    {
                        outcome = PublishRaftIdOutcome.SUCCESSFUL_PUBLISH_BY_OTHER;
                    }
                    request.replyTo().tell( outcome, getSelf() );
                } );
    }

    private void handleUpdateFailure( Replicator.UpdateFailure<?> updateFailure )
    {
        updateFailure.getRequest()
                .filter( m -> m instanceof RaftIdSetRequest )
                .map( m -> (RaftIdSetRequest) m )
                .ifPresent( request ->
                {
                    String message = String.format( "Failed to set RaftId with request: %s", request );
                    log().warning( message );
                    request.replyTo().tell( PublishRaftIdOutcome.FAILED_PUBLISH, getSelf() );
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
