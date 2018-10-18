/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.actor.Props;
import akka.actor.ProviderSelection;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.client.ClusterClientSettings;
import akka.cluster.ddata.DistributedData;
import akka.japi.function.Procedure;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Wraps an actor system and top level actors and streams. Gracefully stops everything. Create an actor system first.
 */
public class ActorSystemLifecycle
{
    public static final int SHUTDOWN_TIMEOUT_S = 15;

    private final ActorSystemFactory actorSystemFactory;
    private final Log log;

    private List<SourceQueueWithComplete<?>> queues = new ArrayList<>();
    private List<ActorRef> actors = new ArrayList<>();
    private Cluster cluster;
    private ActorRef replicator;
    private ActorMaterializer materializer;
    private ActorSystem actorSystem;
    private ClusterClientReceptionist clusterClientReceptionist;
    private ClusterClientSettings clusterClientSettings;

    public ActorSystemLifecycle( ActorSystemFactory actorSystemFactory, LogProvider logProvider )
    {
        this.actorSystemFactory = actorSystemFactory;
        this.log = logProvider.getLog( getClass() );
    }

    public void createClusterActorSystem()
    {
        this.actorSystem =  actorSystemFactory.createActorSystem( ProviderSelection.cluster() );
    }

    public void createClientActorSystem()
    {
        this.actorSystem = actorSystemFactory.createActorSystem( ProviderSelection.remote() );
    }

    public void stop()
    {
        gracefullyStopActors();
        completeQueues();
    }

    public void shutdown() throws Throwable
    {
        try
        {
            CoordinatedShutdown.get( actorSystem ).runAll().toCompletableFuture().get( SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            log.warn( "Exception shutting down actor system", e );
            throw e;
        }
        finally
        {
            actorSystem = null;
        }
    }

    private void gracefullyStopActors()
    {
        List<CompletableFuture<Boolean>> futures = actors.stream()
                .map( this::startGracefulShutdown )
                .collect( Collectors.toList() );

        futures.forEach( this::completeGracefulShutdown );

        actors.clear();
    }

    private void completeQueues()
    {
        queues.forEach( SourceQueueWithComplete::complete );

        queues.clear();
    }

    private CompletableFuture<Boolean> startGracefulShutdown( ActorRef actor )
    {
        return PatternsCS.gracefulStop( actor, Duration.ofSeconds( SHUTDOWN_TIMEOUT_S ) ).toCompletableFuture();
    }

    private void completeGracefulShutdown( CompletableFuture<Boolean> future )
    {
        try
        {
            future.get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            log.warn( "Exception gracefully shutting down actor", e );
        }
    }

    public Cluster cluster()
    {
        if ( cluster == null )
        {
            cluster = Cluster.get( actorSystem );
        }
        return cluster;
    }

    public ActorRef replicator()
    {
        if ( replicator == null )
        {
            replicator = DistributedData.get( actorSystem ).replicator();
        }
        return replicator;
    }

    public ClusterClientReceptionist clusterClientReceptionist()
    {
        if ( clusterClientReceptionist == null )
        {
            clusterClientReceptionist = ClusterClientReceptionist.get( actorSystem );
        }
        return clusterClientReceptionist;
    }

    private ActorMaterializer materializer()
    {
        if ( materializer == null )
        {
            ActorMaterializerSettings settings = ActorMaterializerSettings.create( actorSystem )
                    .withDispatcher( TypesafeConfigService.DISCOVERY_SINK_DISPATCHER );
            materializer = ActorMaterializer.create( settings, actorSystem );
        }
        return materializer;
    }

    public ClusterClientSettings clusterClientSettings()
    {
        if ( clusterClientSettings == null )
        {
            clusterClientSettings = ClusterClientSettings.create( actorSystem ).withInitialContacts( actorSystemFactory.initialClientContacts() );
        }
        return clusterClientSettings;
    }

    public <T> SourceQueueWithComplete<T> queueMostRecent( Procedure<T> sink )
    {
        SourceQueueWithComplete<T> queue = Source.<T>queue( 1, OverflowStrategy.dropHead() )
                .to( Sink.foreach( sink ) )
                .run( materializer() );

        queues.add( queue );

        return queue;
    }

    public ActorRef actorOf( Props props, String name )
    {
        ActorRef actor = actorSystem.actorOf( props, name );
        actors.add( actor );
        return actor;
    }
}
