/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.Done;
import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.CoordinatedShutdown;
import akka.actor.Props;
import akka.actor.ProviderSelection;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.client.ClusterClientSettings;
import akka.event.EventStream;
import akka.japi.function.Procedure;
import akka.pattern.Patterns;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

/**
 * Wraps an actor system and top level actors and streams. Gracefully stops everything on shutdown. Create an actor system first.
 */
public class ActorSystemLifecycle
{
    static final int SYSTEM_SHUTDOWN_TIMEOUT_S = 60;
    static final int ACTOR_SHUTDOWN_TIMEOUT_S = 15;

    private final ActorSystemFactory actorSystemFactory;
    private final RemoteMembersResolver resolver;
    private final JoinMessageFactory joinMessageFactory;
    private final Config config;
    private final Log log;

    @VisibleForTesting
    protected ActorSystemComponents actorSystemComponents;

    public ActorSystemLifecycle( ActorSystemFactory actorSystemFactory, RemoteMembersResolver resolver, JoinMessageFactory joinMessageFactory, Config config,
            LogProvider logProvider )
    {
        this.actorSystemFactory = actorSystemFactory;
        this.resolver = resolver;
        this.joinMessageFactory = joinMessageFactory;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    public void createClusterActorSystem()
    {
        this.actorSystemComponents = new ActorSystemComponents( actorSystemFactory,  ProviderSelection.cluster() );
        Props props = ClusterJoiningActor.props( cluster(), resolver, config );
        applicationActorOf( props, ClusterJoiningActor.NAME ).tell( joinMessageFactory.message(), ActorRef.noSender() );
    }

    public void createClientActorSystem()
    {
        this.actorSystemComponents = new ActorSystemComponents( actorSystemFactory, ProviderSelection.remote() );
    }

    public void addSeenAddresses( Collection<Address> addresses )
    {
        joinMessageFactory.addSeenAddresses( addresses );
    }

    public void shutdown() throws Exception
    {
        if ( actorSystemComponents == null )
        {
            return;
        }

        try
        {
            doShutdown( actorSystemComponents );
        }
        catch ( Exception e )
        {
            log.warn( "Exception shutting down actor system", e );
            throw e;
        }
        finally
        {
            LoggingActor.disable( actorSystemComponents.actorSystem() );
            actorSystemComponents = null;
        }
    }

    @VisibleForTesting
    void doShutdown( ActorSystemComponents actorSystemComponents ) throws Exception
    {
        actorSystemComponents
                .coordinatedShutdown()
                .runAll( ShutdownByNeo4jLifecycle.INSTANCE )
                .toCompletableFuture()
                .get( SYSTEM_SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS );
    }

    public <T> SourceQueueWithComplete<T> queueMostRecent( Procedure<T> sink )
    {
        SourceQueueWithComplete<T> queue = Source.<T>queue( 1, OverflowStrategy.dropHead() )
                .to( Sink.foreach( sink ) )
                .run( actorSystemComponents.materializer() );

        actorSystemComponents.coordinatedShutdown()
                .addTask( CoordinatedShutdown.PhaseServiceStop(), "queue-" + UUID.randomUUID(), () -> completeQueue( queue ) );

        return queue;
    }

    private <T> CompletionStage<Done> completeQueue( SourceQueueWithComplete<T> queue )
    {
        queue.complete();
        return queue.watchCompletion();
    }

    public ActorRef applicationActorOf( Props props, String name )
    {
        ActorRef actorRef = actorSystemComponents.actorSystem().actorOf( props, name );
        actorSystemComponents.coordinatedShutdown()
                .addTask( CoordinatedShutdown.PhaseServiceUnbind(), name + "-shutdown", () -> gracefulShutdown( actorRef ) );
        return actorRef;
    }

    public ActorRef systemActorOf( Props props, String name )
    {
        return actorSystemComponents.actorSystem().actorOf( props, name );
    }

    private CompletionStage<Done> gracefulShutdown( ActorRef actor )
    {
        return Patterns.gracefulStop( actor, Duration.ofSeconds( ACTOR_SHUTDOWN_TIMEOUT_S ) )
                       .thenApplyAsync( ignored -> Done.done(), this.actorSystemComponents.actorSystem().dispatcher() );
    }

    public EventStream eventStream()
    {
        return actorSystemComponents.actorSystem().eventStream();
    }

    private static class ShutdownByNeo4jLifecycle implements CoordinatedShutdown.Reason
    {
        static ShutdownByNeo4jLifecycle INSTANCE = new ShutdownByNeo4jLifecycle();
    }

    public Cluster cluster()
    {
        return actorSystemComponents.cluster();
    }

    public ActorRef replicator()
    {
        return actorSystemComponents.replicator();
    }

    public ClusterClientReceptionist clusterClientReceptionist()
    {
        return actorSystemComponents.clusterClientReceptionist();
    }

    public ClusterClientSettings clusterClientSettings()
    {
        Set<ActorPath> actorPaths = resolver.resolve( this::toActorPath, HashSet::new );

        return ClusterClientSettings.create( actorSystemComponents.actorSystem() ).withInitialContacts( actorPaths );
    }

    private ActorPath toActorPath( SocketAddress addr )
    {
        String path = String.format( "%s://%s@%s/system/receptionist", ClusterJoiningActor.AKKA_SCHEME, ActorSystemFactory.ACTOR_SYSTEM_NAME, addr.toString() );
        return ActorPaths.fromString( path );
    }
}
