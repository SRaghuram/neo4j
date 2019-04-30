/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.UniqueAddress;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import scala.concurrent.duration.FiniteDuration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class BaseReplicatedDataActor<T extends ReplicatedData> extends AbstractActor
{
    private static final Replicator.WriteConsistency WRITE_CONSISTENCY = new Replicator.WriteAll( new FiniteDuration( 10, TimeUnit.SECONDS ) );

    protected final Cluster cluster;
    protected final ActorRef replicator;
    protected final Key<T> key;
    protected final Supplier<T> emptyData;
    protected T data;
    protected final Log log;

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
        removeDataFromReplicator( cluster.selfUniqueAddress() );
    }

    protected abstract void removeDataFromReplicator( UniqueAddress uniqueAddress );

    @Override
    public final Receive createReceive()
    {
        ReceiveBuilder receiveBuilder = new ReceiveBuilder();
        handleCustomEvents( receiveBuilder );
        handleReplicationEvents( receiveBuilder );
        return receiveBuilder.build();
    }

    @SuppressWarnings( "unchecked" )
    private void handleReplicationEvents( ReceiveBuilder builder )
    {
        builder.match( Replicator.Changed.class, c -> c.key().equals( key ), message ->
            {
                T newData = (T) message.dataValue();
                handleIncomingData( newData );
            } ).match( Replicator.UpdateResponse.class, updated -> log.debug( "Update: %s", updated ) );
    }

    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        //no-op by default
    }

    protected abstract void handleIncomingData( T newData );

    private void subscribeToReplicatorEvents( Replicator.ReplicatorMessage message )
    {
        replicator.tell( message, getSelf() );
    }

    protected void modifyReplicatedData( Key<T> key, Function<T,T> modify )
    {
        modifyReplicatedData( key, modify, null );
    }

    protected <M> void modifyReplicatedData( Key<T> key, Function<T,T> modify, M message )
    {
        Replicator.Update<T> update = new Replicator.Update<>( key, emptyData.get(), WRITE_CONSISTENCY, Optional.ofNullable(message), modify );

        replicator.tell( update, self() );
    }

}
