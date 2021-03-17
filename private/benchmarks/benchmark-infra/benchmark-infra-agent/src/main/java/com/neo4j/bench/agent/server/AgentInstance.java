/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.client.PrepareRequest;
import com.neo4j.bench.agent.client.StartDatabaseRequest;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class AgentInstance
{
    public static final String STARTED_MESSAGE = "Agent started";

    private static final Logger LOG = LoggerFactory.getLogger( AgentInstance.class );

    private final AtomicBoolean running = new AtomicBoolean();
    private final int port;
    private final AgentLifecycle agentLifecycle;

    private Service webServer;

    public AgentInstance( int port,
                          Path workDirectory,
                          Supplier<ArtifactStorage> artifactStorageFactory,
                          Function<Path,DatabaseServerWrapper> databaseServerWrapperFactory )
    {
        this.port = port;
        this.agentLifecycle = new AgentLifecycle( workDirectory, artifactStorageFactory, databaseServerWrapperFactory );
    }

    public boolean isRunning()
    {
        return running.get();
    }

    public int port()
    {
        return port;
    }

    /* Web interface / Agent lifecycle commands */

    public synchronized void start()
    {
        if ( isRunning() )
        {
            LOG.error( "Already running!" );
            return;
        }
        webServer = Service.ignite();

        webServer.port( port );
        setupRoutes();

        LOG.info( "Agent instance starting on {} using directory {}", port, agentLifecycle.workspace() );
        webServer.awaitInitialization();
        LOG.info( STARTED_MESSAGE );
        running.set( true );
    }

    public void stop()
    {
        if ( !running.compareAndSet( true, false ) )
        {
            LOG.error( "Stop called the second time!" );
            return;
        }
        LOG.debug( "Wait 500 ms before stopping" );
        // Execution will halt here for half a second, reason is that this stop is generally invoked my the REST API ( @stopAgent ).
        //  The REST API will call this method in a different background thread. We need to make sure that the response for the call is returned all the way
        //  before we kill of the http server. The half second is arbitary value and it should be enough to respond.
        //  This is by no means elegant or even safe - but drastically reduces the chance of the race.
        try
        {
            Thread.sleep( 500 );
        }
        catch ( InterruptedException e )
        {
            // no-op
        }

        webServer.stop();
        LOG.debug( "Stop signal sent, await stop" );
        webServer.awaitStop();
        webServer = null;
        LOG.debug( "Stop finished, deleting work directory" );
        agentLifecycle.close();
        LOG.info( "Agent instance terminated - JAVA VM should exit now" );
    }

    /* API commands */

    private String ping()
    {
        return "I am alive!";
    }

    private String state( Response response )
    {
        response.type( "application/json; charset=utf-8" );
        return agentLifecycle.agentState();
    }

    private String stopAgent()
    {
        CompletableFuture.runAsync( this::stop );
        return "Will stop soon!";
    }

    private String prepare( Request request, Response response )
    {
        PrepareRequest parameters = JsonUtil.deserializeJson( request.body(), PrepareRequest.class );
        new PrepareCommand( agentLifecycle, parameters ).get();
        return "Preparation done!";
    }

    private String startDatabase( Request request, Response response )
    {
        StartDatabaseRequest parameters = JsonUtil.deserializeJson( request.body(), StartDatabaseRequest.class );
        return new StartDatabaseCommand( agentLifecycle, parameters ).get().toString();
    }

    private String stopDatabase( Request request, Response response )
    {
        return new StopDatabaseCommand( agentLifecycle ).get();
    }

    private void setupRoutes()
    {
        Path staticFilesDirectory = agentLifecycle.workspace().resolve( "static" );
        BenchmarkUtil.tryMkDir( staticFilesDirectory );
        webServer.externalStaticFileLocation( staticFilesDirectory.toString() );

        webServer.before( "/*", ( request, response ) -> LOG.info( "Request to: {} {}", request.requestMethod(), request.pathInfo() ) );
        webServer.get( "/ping", ( request, response ) -> this.ping() );
        webServer.get( "/state", ( request, response ) -> this.state( response ) );
        webServer.post( "/stopAgent", ( request, response ) -> this.stopAgent() );
        webServer.post( "/prepare", ( request, response ) -> this.safe( request, response, this::prepare ) );
        webServer.post( "/startDatabase", ( request, response ) -> this.safe( request, response, this::startDatabase ) );
        webServer.post( "/stopDatabase", ( request, response ) -> this.safe( request, response, this::stopDatabase ) );
    }

    private String safe( Request request, Response response, BiFunction<Request,Response,String> method )
    {
        try
        {
            return method.apply( request, response );
        }
        catch ( IllegalStateException e )
        {
            LOG.warn( "Illegal state call: {}", e.getMessage() );
            response.type( "text/plain" );
            response.status( HttpURLConnection.HTTP_CONFLICT );
            return e.getMessage();
        }
        catch ( Exception e )
        {
            LOG.warn( "Error during performing", e );
            response.type( "text/plain" );
            response.status( HttpURLConnection.HTTP_INTERNAL_ERROR );
            return e.getMessage();
        }
    }
}
