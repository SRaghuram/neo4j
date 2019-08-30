/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.harness.PortAuthorityPortPickingStrategy;
import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.CommercialInProcessNeo4jBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.platform.commons.util.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.extension.Neo4j;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodySubscribers.mapping;
import static java.net.http.HttpResponse.BodySubscribers.ofString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

final class CausalClusterRestEndpointHelpers
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private static final HttpClient HTTP_CLIENT = newHttpClient();
    private static final HttpClient REDIRECT_HTTP_CLIENT = newBuilder().followRedirects( HttpClient.Redirect.NORMAL ).build();

    private CausalClusterRestEndpointHelpers()
    {
    }

    static void writeSomeData( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        GraphDatabaseService db = getLeader( cluster ).defaultDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( "MyNode" ) );
            tx.commit();
        }
    }

    static CausalClusterInProcessBuilder.CausalCluster startCluster( TestDirectory testDirectory )
    {
        var clusterDirectory = testDirectory.directory( "CLUSTER" );
        var cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( CommercialInProcessNeo4jBuilder::new )
                .withCores( 3 )
                .withReplicas( 2 )
                .withLogger( LOG_PROVIDER )
                .atPath( clusterDirectory.toPath() )
                .withOptionalPortsStrategy( new PortAuthorityPortPickingStrategy() )
                .build();

        try
        {
            cluster.boot();
            return cluster;
        }
        catch ( Throwable bootError )
        {
            try
            {
                cluster.shutdown();
            }
            catch ( Throwable shutdownError )
            {
                bootError.addSuppressed( shutdownError );
            }
            throw bootError;
        }
    }

    static Neo4j getLeader( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        return cluster.getCores()
                .stream()
                .filter( core -> Role.LEADER.equals( getCurrentCoreRole( core ) ) )
                .findAny()
                .orElseThrow( () -> new IllegalStateException( "Leader does not exist" ) );
    }

    private static Role getCurrentCoreRole( Neo4j core )
    {
        return ((GraphDatabaseFacade) core.defaultDatabaseService()).getDependencyResolver().resolveDependency( RoleProvider.class ).currentRole();
    }

    static HttpResponse<Map<String,Object>> queryClusterEndpoint( Neo4j server, String databaseName )
    {
        return sendGET( clusterEndpoint( server, databaseName ), ofJson() );
    }

    static HttpResponse<Map<String,Object>> queryLegacyClusterEndpoint( Neo4j server )
    {
        return sendGET( legacyClusterEndpoint( server ), ofJson(), true );
    }

    static HttpResponse<Boolean> queryLegacyClusterEndpoint( Neo4j server, String path )
    {
        return sendGET( legacyClusterEndpoint( server, path ), ofBoolean(), true );
    }

    static HttpResponse<Map<String,Object>> queryLegacyClusterStatusEndpoint( Neo4j server )
    {
        return sendGET( legacyClusterEndpoint( server, "status" ), ofJson(), true );
    }

    static HttpResponse<Boolean> queryAvailabilityEndpoint( Neo4j server, String databaseName )
    {
        return queryBooleanEndpoint( availabilityEndpoint( server, databaseName ) );
    }

    static HttpResponse<Boolean> queryWritableEndpoint( Neo4j server, String databaseName )
    {
        return queryBooleanEndpoint( writableEndpoint( server, databaseName ) );
    }

    static HttpResponse<Boolean> queryReadOnlyEndpoint( Neo4j server, String databaseName )
    {
        return queryBooleanEndpoint( readOnlyEndpoint( server, databaseName ) );
    }

    static HttpResponse<Map<String,Object>> queryStatusEndpoint( Neo4j server, String databaseName )
    {
        return sendGET( statusEndpoint( server, databaseName ), ofJson() );
    }

    private static URI legacyClusterEndpoint( Neo4j server )
    {
        return server.httpURI().resolve( "/db/manage/server/causalclustering" );
    }

    private static URI legacyClusterEndpoint( Neo4j server, String path )
    {
        return server.httpURI().resolve( "/db/manage/server/causalclustering/" + path );
    }

    private static URI clusterEndpoint( Neo4j server, String databaseName )
    {
        return server.httpURI().resolve( "/db/" + databaseName + "/cluster" );
    }

    private static URI readOnlyEndpoint( Neo4j server, String databaseName )
    {
        return server.httpURI().resolve( "/db/" + databaseName + "/cluster/read-only" );
    }

    private static URI writableEndpoint( Neo4j server, String databaseName )
    {
        return server.httpURI().resolve( "/db/" + databaseName + "/cluster/writable" );
    }

    private static URI availabilityEndpoint( Neo4j server, String databaseName )
    {
        return server.httpURI().resolve( "/db/" + databaseName + "/cluster/available" );
    }

    private static URI statusEndpoint( Neo4j server, String databaseName )
    {
        return server.httpURI().resolve( "/db/" + databaseName + "/cluster/status" );
    }

    private static HttpResponse<Boolean> queryBooleanEndpoint( URI uri )
    {
        return sendGET( uri, ofBoolean() );
    }

    private static <T> HttpResponse<T> sendGET( URI uri, BodyHandler<T> bodyHandler )
    {
        return sendGET( uri, bodyHandler, false );
    }

    private static <T> HttpResponse<T> sendGET( URI uri, BodyHandler<T> bodyHandler, boolean allowsRedirect )
    {
        var request = HttpRequest.newBuilder( uri )
                .header( ACCEPT, APPLICATION_JSON )
                .GET()
                .build();

        var client = allowsRedirect ? REDIRECT_HTTP_CLIENT : HTTP_CLIENT;
        try
        {
            return client.send( request, bodyHandler );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    private static BodyHandler<Boolean> ofBoolean()
    {
        return responseInfo -> mapping( ofString( UTF_8 ), Boolean::valueOf );
    }

    private static BodyHandler<Map<String,Object>> ofJson()
    {
        return responseInfo -> mapping( ofString( UTF_8 ), CausalClusterRestEndpointHelpers::readJson );
    }

    @SuppressWarnings( "unchecked" )
    private static Map<String,Object> readJson( String str )
    {
        if ( StringUtils.isBlank( str ) )
        {
            return emptyMap();
        }
        try
        {
            return OBJECT_MAPPER.readValue( str, Map.class );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
