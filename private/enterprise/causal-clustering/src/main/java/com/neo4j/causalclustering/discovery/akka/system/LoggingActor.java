/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.dispatch.RequiresMessageQueue;
import akka.event.LoggerMessageQueueSemantics;
import akka.event.Logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class LoggingActor extends AbstractActor implements RequiresMessageQueue<LoggerMessageQueueSemantics>
{
    private static final Map<ActorSystem,LogProvider> LOG_PROVIDERS = new HashMap<>();
    private static LogProvider defaultLogProvider = NullLogProvider.getInstance();

    /**
     * Configure Akka logging to use {@link LogProvider}. Integration tests may have multiple database instances sharing this class:
     * a mapping of ({@link ActorSystem} -> {@link LogProvider}) is maintained to get the log messages into the correct log files.
     * @param system Uniquely identifies database instance
     * @param logProvider Appropriate for identified instance
     */
    static void enable( ActorSystem system, LogProvider logProvider )
    {
        LoggingActor.LOG_PROVIDERS.put( system, logProvider );
        logProvider.getLog( LoggingActor.class )
                .debug( "Added logProvider for %s. %d LogProviders and ActorSystems remaining.", system.name(), LOG_PROVIDERS.size() );
    }

    /**
     * If {@link LoggingActor#enable(ActorSystem, LogProvider)} has not been called yet provide a log provider. In this case logs in
     * integration tests may not end up in the expected file.
     * @param logProvider To use if no instance specific log provider can be found
     */
    static void enable( LogProvider logProvider )
    {
        LoggingActor.defaultLogProvider = logProvider;
    }

    /**
     * Remove association of ActorSystem to LogProvider. This is usually performed during actor system shutdown.
     * Failure to call this in integration tests may cause a memory leak.
     * @param system the actor system for which to disable logging
     */
    static void disable( ActorSystem system )
    {
        Optional<LogProvider> removed = Optional.ofNullable( LOG_PROVIDERS.remove( system ) );
        removed.ifPresent( log -> log.getLog( LoggingActor.class )
                .debug( "Removed logProvider for %s. %d LogProviders and ActorSystems remaining.", system.name(), LOG_PROVIDERS.size() ) );
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match( Logging.Error.class, error -> getLog( error.logClass() ).error( getMessage( error ), error.cause() ) )
                .match( Logging.Warning.class, warning -> getLog( warning.logClass() ).warn( getMessage( warning ) ) )
                .match( Logging.Info.class, info -> getLog( info.logClass() ).info( getMessage( info ) ) )
                .match( Logging.Debug.class, debug -> getLog( debug.logClass() ).debug( getMessage( debug ) ) )
                .match( Logging.InitializeLogger.class, ignored -> sender().tell( Logging.loggerInitialized(), self() ) )
                .build();
    }

    private Log getLog( Class loggingClass )
    {
        LogProvider configuredLogProvider = LOG_PROVIDERS.get( getContext().system() );
        LogProvider logProvider = configuredLogProvider == null ? defaultLogProvider : configuredLogProvider;
        return logProvider.getLog( loggingClass );
    }

    private String getMessage( Logging.LogEvent error )
    {
        return error.message() == null ? "null" : error.message().toString();
    }
}
