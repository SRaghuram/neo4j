/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.Replicator;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class BaseReplicatedDataActor<T extends ReplicatedData> extends AbstractActorWithTimersAndLogging
{
    private static final Replicator.WriteConsistency METADATA_CONSISTENCY = new Replicator.WriteAll( new FiniteDuration( 10, TimeUnit.SECONDS ) );
    private static final String METRIC_TIMER_KEY = "refresh metric";

    protected final Cluster cluster;
    protected final ActorRef replicator;
    protected final Key<T> key;
    protected T data;
    private final ReplicatedDataIdentifier identifier;
    private final Supplier<T> emptyData;
    private final ReplicatedDataMonitor monitor;

    protected BaseReplicatedDataActor( Cluster cluster, ActorRef replicator, Function<String,Key<T>> keyFunction, Supplier<T> emptyData,
            ReplicatedDataIdentifier identifier, ReplicatedDataMonitor monitor )
    {
        this.cluster = cluster;
        this.replicator = replicator;
        this.key = keyFunction.apply( identifier.keyName() );
        this.emptyData = emptyData;
        this.data = emptyData.get();
        this.identifier = identifier;
        this.monitor = monitor;
    }

    @Override
    public final void preStart()
    {
        sendInitialDataToReplicator();
        subscribeToReplicatorEvents( new Replicator.Subscribe<>( key, getSelf() ) );
        getTimers().startPeriodicTimer( METRIC_TIMER_KEY, Tick.getInstance(), Duration.ofMinutes( 1 ) );
    }

    protected abstract void sendInitialDataToReplicator();

    @Override
    public final void postStop()
    {
        subscribeToReplicatorEvents( new Replicator.Unsubscribe<>( key, getSelf() ) );
    }

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
                logDataMetric();
            } ).match( Replicator.UpdateResponse.class, updated -> log().debug( "Update: {}", updated ) )
               .match( Tick.class, ignored -> logDataMetric() );
    }

    protected abstract void handleCustomEvents( ReceiveBuilder builder );

    protected abstract void handleIncomingData( T newData );

    private void logDataMetric()
    {
        monitor.setVisibleDataSize( identifier, dataMetricVisible() );
        monitor.setInvisibleDataSize( identifier, dataMetricInvisible() );
    }

    protected abstract int dataMetricVisible();

    protected abstract int dataMetricInvisible();

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
