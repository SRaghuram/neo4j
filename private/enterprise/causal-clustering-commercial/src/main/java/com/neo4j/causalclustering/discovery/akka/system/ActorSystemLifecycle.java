/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.Done;
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
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Wraps an actor system and top level actors and streams. Gracefully stops everything on shutdown. Create an actor system first.
 */
public class ActorSystemLifecycle
{
    private static final int SHUTDOWN_TIMEOUT_S = 15;

    private final ActorSystemFactory actorSystemFactory;
    private final Log log;

    private Cluster cluster;
    private ActorRef replicator;
    private ActorMaterializer materializer;
    private ActorSystem actorSystem;
    private ClusterClientReceptionist clusterClientReceptionist;
    private ClusterClientSettings clusterClientSettings;
    private CoordinatedShutdown coordinatedShutdown;

    public ActorSystemLifecycle( ActorSystemFactory actorSystemFactory, LogProvider logProvider )
    {
        this.actorSystemFactory = actorSystemFactory;
        this.log = logProvider.getLog( getClass() );
    }

    public void createClusterActorSystem()
    {
        createActorSystem(ProviderSelection.cluster() );
    }

    public void createClientActorSystem()
    {
        createActorSystem( ProviderSelection.remote() );
    }

    private void createActorSystem( ProviderSelection remote )
    {
        this.actorSystem = actorSystemFactory.createActorSystem( remote );
        this.coordinatedShutdown = CoordinatedShutdown.get( actorSystem );
    }

    public void shutdown() throws Throwable
    {
        try
        {
            CoordinatedShutdown
                    .get( actorSystem )
                    .runAll( ShutdownByNeo4jLifecycle.INSTANCE )
                    .toCompletableFuture()
                    .get( SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            log.warn( "Exception shutting down actor system", e );
            throw e;
        }
        finally
        {
            LoggingActor.disable( actorSystem );
            actorSystem = null;
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

        coordinatedShutdown.addTask( CoordinatedShutdown.PhaseServiceStop(), "queue-" + UUID.randomUUID(), () -> completeQueue( queue ) );

        return queue;
    }

    private <T> CompletionStage<Done> completeQueue( SourceQueueWithComplete<T> queue )
    {
        queue.complete();
        return queue.watchCompletion();
    }

    public ActorRef applicationActorOf( Props props, String name )
    {
        ActorRef actorRef = actorSystem.actorOf( props, name );
        coordinatedShutdown.addTask( CoordinatedShutdown.PhaseServiceUnbind(), name + "-shutdown", () -> gracefulShutdown( actorRef ) );
        return actorRef;
    }

    public ActorRef systemActorOf( Props props, String name )
    {
        return actorSystem.actorOf( props, name );
    }

    private CompletionStage<Done> gracefulShutdown( ActorRef actor )
    {
        return PatternsCS.gracefulStop( actor, Duration.ofSeconds( SHUTDOWN_TIMEOUT_S ) ).thenApply( ignored -> Done.done() );
    }

    private static class ShutdownByNeo4jLifecycle implements CoordinatedShutdown.Reason
    {
        public static ShutdownByNeo4jLifecycle INSTANCE = new ShutdownByNeo4jLifecycle();
    }
}
