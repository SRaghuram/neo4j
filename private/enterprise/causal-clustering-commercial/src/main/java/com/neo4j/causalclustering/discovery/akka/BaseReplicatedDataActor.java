/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;

import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.discovery.akka.CoreTopologyActor.METADATA_CONSISTENCY;

public abstract class BaseReplicatedDataActor<T extends ReplicatedData> extends AbstractActor
{
    final Cluster cluster;
    final ActorRef replicator;
    final Key<T> key;
    final Supplier<T> emptyData;
    T data;
    final Log log;

    protected BaseReplicatedDataActor( Cluster cluster, ActorRef replicator, Key<T> key, Supplier<T> emptyData, LogProvider logProvider )
    {
        this.cluster = cluster;
        this.replicator = replicator;
        this.key = key;
        this.emptyData = emptyData;
        this.data = emptyData.get();
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public final void preStart()
    {
        sendInitialDataToReplicator();
        subscribeToReplicatorEvents( new Replicator.Subscribe<>( key, getSelf() ) );
    }

    protected abstract void sendInitialDataToReplicator();

    @Override
    public final void postStop()
    {
        subscribeToReplicatorEvents( new Replicator.Unsubscribe<>( key, getSelf() ) );
        removeDataFromReplicator();
    }

    protected abstract void removeDataFromReplicator();

    @Override
    public final Receive createReceive()
    {
        ReceiveBuilder receiveBuilder = new ReceiveBuilder();
        handleReplicationEvents( receiveBuilder );
        handleCustomEvents( receiveBuilder );
        return receiveBuilder.build();
    }

    @SuppressWarnings( "unchecked" )
    private void handleReplicationEvents( ReceiveBuilder builder )
    {
        builder.match( Replicator.Changed.class, c -> c.key().equals( key ), message ->
            {
                T newData = (T) message.dataValue();
                handleIncomingData( newData );
            } ).match( Replicator.UpdateResponse.class, updated -> {
                log.debug( "Update: %s", updated );
            } );
    }

    protected void handleCustomEvents( ReceiveBuilder builder )
    {

    }

    protected abstract void handleIncomingData( T newData );

    private void subscribeToReplicatorEvents( Replicator.ReplicatorMessage message )
    {
        replicator.tell( message, getSelf() );
    }

    protected void modifyReplicatedData( Key<T> key, Function<T,T> modify )
    {
        Replicator.Update<T> update = new Replicator.Update<>( key, emptyData.get(), METADATA_CONSISTENCY, modify );

        replicator.tell( update, self() );
    }
}
