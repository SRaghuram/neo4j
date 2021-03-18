/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.client;

import com.neo4j.bench.agent.Agent;
import com.neo4j.bench.agent.AgentState;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.util.Retrier;
import com.neo4j.bench.infra.Extractor;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class AgentClient implements Agent
{
    private static final Logger LOG = LoggerFactory.getLogger( AgentClient.class );
    private static final Duration VALIDATION_DURATION = Duration.ofSeconds( 15 );
    private static final Consumer<HttpRequest.Builder> EMPTY_BODY =
            builder -> builder.POST( HttpRequest.BodyPublishers.noBody() ).header( "Content-type", "text/plain" );

    public static Agent bootstrap( InetAddress inetAddress,
                                   Path pathToAgent ) throws IOException
    {
        return bootstrap( inetAddress,
                          pathToAgent,
                          AgentDeployer.DEFAULT_SSH_PORT,
                          DEFAULT_PORT );
    }

    public static Agent bootstrap( InetAddress inetAddress,
                                   Path pathToAgent,
                                   int advertisedSshPort,
                                   int advertisedWebPort ) throws IOException
    {
        AgentDeployer agentDeployer = new AgentDeployer( inetAddress,
                                                         pathToAgent,
                                                         advertisedSshPort,
                                                         advertisedWebPort );
        URI agentUri = agentDeployer.deploy( emptyList() );
        return client( agentUri );
    }

    public static Agent client( URI agentUri ) throws IOException
    {
        return new AgentClient( agentUri ).validate();
    }

    private final HttpClient httpClient;
    private final URI remoteUri;
    private String lastArchiveName;

    public AgentClient( URI uri )
    {
        this.remoteUri = uri;
        this.httpClient = HttpClient.newBuilder().connectTimeout( Duration.ofSeconds( 1 ) ).build();
    }

    @Override
    public boolean ping()
    {
        try
        {
            return barePing();
        }
        catch ( IOException | InterruptedException e )
        {
            LOG.error( "Ping unsuccessful", e );
        }
        return false;
    }

    @Override
    public AgentState state()
    {
        var result = callAndParse( "state", HttpRequest.Builder::GET, Function.identity() );
        LOG.debug( "Agent State of {} is {}", remoteUri, result );
        return JsonUtil.deserializeJson( result, AgentState.class );
    }

    @Override
    public boolean stopAgent()
    {
        return callAndParse( "stopAgent", EMPTY_BODY, ignored -> true );
    }

    @Override
    public boolean prepare( URI artifactBaseUri,
                            String productArchive,
                            URI dataSetBaseUri,
                            String datasetName,
                            Version neo4jVersion )
    {
        PrepareRequest prepareRequest = PrepareRequest.from( artifactBaseUri,
                                                             productArchive,
                                                             dataSetBaseUri,
                                                             datasetName,
                                                             neo4jVersion );
        return postAndParse( "prepare", prepareRequest, ignored -> true );
    }

    @Override
    public URI startDatabase( String placeHolderForProfilerInfos,
                              boolean copyStore,
                              Neo4jConfig neo4jConfig )
    {
        lastArchiveName = null;
        StartDatabaseRequest startDatabaseRequest = StartDatabaseRequest.from( placeHolderForProfilerInfos,
                                                                               copyStore,
                                                                               neo4jConfig );
        return postAndParse( "startDatabase", startDatabaseRequest, URI::create );
    }

    @Override
    public void stopDatabase()
    {
        lastArchiveName = callAndParse( "stopDatabase", EMPTY_BODY, Function.identity() );
        LOG.info( "Remote database stopped, results can be pulled" );
    }

    @Override
    public void downloadResults( Path downloadResultsInto )
    {
        if ( lastArchiveName == null )
        {
            throw new IllegalStateException( "Database is not yet stopped, no results exist yet!" );
        }
        var input = call( lastArchiveName, HttpRequest.Builder::GET, HttpResponse.BodyHandlers.ofInputStream() );
        Extractor.extract( downloadResultsInto, input );
    }

    /* Helpers */

    private Agent validate() throws IOException
    {
        new Retrier( VALIDATION_DURATION ).retryUntil( this::quietPing, r -> r, 100 );
        return this;
    }

    private <T> T postAndParse( String command, Object postBody, Function<String,T> responseParser )
    {
        var requestBody = JsonUtil.serializeJson( postBody );
        LOG.debug( "{} {}", command, requestBody );
        return callAndParse( command,
                builder -> builder.POST( HttpRequest.BodyPublishers.ofString( requestBody ) ).header( "Content-Type", "application/json; charset=utf-8" ),
                responseParser );
    }

    private <T> T callAndParse( String command, Consumer<HttpRequest.Builder> requestBuilder, Function<String,T> responseParser )
    {
        return responseParser.apply( call( command, requestBuilder, HttpResponse.BodyHandlers.ofString() ) );
    }

    private <U> U call( String command, Consumer<HttpRequest.Builder> requestBuilder, HttpResponse.BodyHandler<U> responseParser )
    {
        try
        {
            LOG.debug( "Call {}/{}", command, remoteUri );
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                                                     .uri( remoteUri.resolve( command ) )
                                                     .timeout( Duration.ofMinutes(1) );
            requestBuilder.accept( builder );
            HttpRequest request = builder.build();
            HttpResponse<U> response = httpClient.send( request, responseParser );
            int responseCode = response.statusCode();
            if ( responseCode != HttpURLConnection.HTTP_OK )
            {
                throw new IOException( "Response code does not match " + HttpURLConnection.HTTP_OK + "<>" + responseCode );
            }
            LOG.debug( "Call {}/{} finished, wait for parsing", command, remoteUri );
            return response.body();
        }
        catch ( InterruptedException e )
        {
            LOG.error( "Command '{}' call unsuccessful", command, e );
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            LOG.error( "Command '{}' call unsuccessful", command, e );
            throw new UncheckedIOException( e );
        }
    }

    private boolean quietPing()
    {
        try
        {
            return barePing();
        }
        catch ( IOException | InterruptedException e )
        {
            return false;
        }
    }

    private boolean barePing() throws IOException, InterruptedException
    {
        var request = HttpRequest.newBuilder( remoteUri.resolve( "ping" ) ).build();
        var response = httpClient.send( request, HttpResponse.BodyHandlers.discarding() );
        return response.statusCode() == HttpURLConnection.HTTP_OK;
    }

    @FunctionalInterface
    private interface ResponseParser<Response, T>
    {
        T parse( Response responseBody ) throws IOException;
    }
}
