/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import org.junit.platform.commons.util.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;
import java.util.Map;

import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.graphdb.Label;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodySubscribers.mapping;
import static java.net.http.HttpResponse.BodySubscribers.ofString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

final class ClusteringEndpointHelpers
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = newHttpClient();

    private ClusteringEndpointHelpers()
    {
    }

    static void writeSomeData( Cluster cluster, String databaseName ) throws Exception
    {
        cluster.coreTx( databaseName, ( db, tx ) ->
        {
            tx.createNode( Label.label( "MyNode" ) );
            tx.commit();
        });
    }

    static URI httpURI( ClusterMember server )
    {
        return URI.create( "http://" + server.config().get( HttpConnector.advertised_address ) );
    }

    static HttpResponse<Map<String,Object>> queryClusterEndpoint( ClusterMember server, String databaseName )
    {
        return sendGET( clusterEndpoint( server, databaseName ), ofJsonObject() );
    }

    static HttpResponse<Map<String,Object>> queryLegacyClusterEndpoint( ClusterMember server )
    {
        return sendGET( legacyClusterEndpoint( server ), ofJsonObject() );
    }

    static HttpResponse<Boolean> queryLegacyClusterEndpoint( ClusterMember server, String path )
    {
        return sendGET( legacyClusterEndpoint( server, path ), ofBoolean() );
    }

    static HttpResponse<Map<String,Object>> queryLegacyClusterStatusEndpoint( ClusterMember server )
    {
        return sendGET( legacyClusterEndpoint( server, "status" ), ofJsonObject() );
    }

    static HttpResponse<Boolean> queryAvailabilityEndpoint( ClusterMember server, String databaseName )
    {
        return queryBooleanEndpoint( availabilityEndpoint( server, databaseName ) );
    }

    static HttpResponse<Boolean> queryWritableEndpoint( ClusterMember server, String databaseName )
    {
        return queryBooleanEndpoint( writableEndpoint( server, databaseName ) );
    }

    static HttpResponse<Boolean> queryReadOnlyEndpoint( ClusterMember server, String databaseName )
    {
        return queryBooleanEndpoint( readOnlyEndpoint( server, databaseName ) );
    }

    static HttpResponse<Map<String,Object>> queryStatusEndpoint( ClusterMember server, String databaseName )
    {
        return sendGET( statusEndpoint( server, databaseName ), ofJsonObject() );
    }

    static HttpResponse<List<Map<String,Object>>> queryCombinedStatusEndpoint( ClusterMember server )
    {
        return sendGET( combinedStatusEndpoint( server ), ofJsonArray() );
    }

    private static URI legacyClusterEndpoint( ClusterMember server )
    {
        return httpURI( server ).resolve( "/db/manage/server/causalclustering" );
    }

    static URI legacyClusterEndpointBase( ClusterMember server )
    {
        return httpURI( server ).resolve( "/db/manage/server/causalclustering/" );
    }

    private static URI legacyClusterEndpoint( ClusterMember server, String path )
    {
        return legacyClusterEndpointBase( server ).resolve( path );
    }

    private static URI clusterEndpoint( ClusterMember server, String databaseName )
    {
        return httpURI( server ).resolve( "/db/" + databaseName + "/cluster" );
    }

    static URI clusterEndpointBase( ClusterMember server, String databaseName )
    {
        return httpURI( server ).resolve( "/db/" + databaseName + "/cluster/" );
    }

    private static URI readOnlyEndpoint( ClusterMember server, String databaseName )
    {
        return clusterEndpointBase( server, databaseName ).resolve( "read-only" );
    }

    private static URI writableEndpoint( ClusterMember server, String databaseName )
    {
        return clusterEndpointBase( server, databaseName ).resolve( "writable" );
    }

    private static URI availabilityEndpoint( ClusterMember server, String databaseName )
    {
        return clusterEndpointBase( server, databaseName ).resolve( "available" );
    }

    private static URI statusEndpoint( ClusterMember server, String databaseName )
    {
        return clusterEndpointBase( server, databaseName ).resolve( "status" );
    }

    private static URI combinedStatusEndpoint( ClusterMember server )
    {
        return httpURI( server ).resolve( "/dbms/cluster/status" );
    }

    private static HttpResponse<Boolean> queryBooleanEndpoint( URI uri )
    {
        return sendGET( uri, ofBoolean() );
    }

    private static <T> HttpResponse<T> sendGET( URI uri, BodyHandler<T> bodyHandler )
    {
        var request = HttpRequest.newBuilder( uri )
                .header( ACCEPT, APPLICATION_JSON )
                .GET()
                .build();

        try
        {
            return HTTP_CLIENT.send( request, bodyHandler );
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

    private static BodyHandler<Map<String,Object>> ofJsonObject()
    {
        return responseInfo -> mapping( ofString( UTF_8 ), ClusteringEndpointHelpers::readJsonObject );
    }

    private static BodyHandler<List<Map<String,Object>>> ofJsonArray()
    {
        return responseInfo -> mapping( ofString( UTF_8 ), ClusteringEndpointHelpers::readJsonArray );
    }

    @SuppressWarnings( "unchecked" )
    private static Map<String,Object> readJsonObject( String str )
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

    @SuppressWarnings( "unchecked" )
    private static List<Map<String,Object>> readJsonArray( String str )
    {
        if ( StringUtils.isBlank( str ) )
        {
            return emptyList();
        }
        try
        {
            return OBJECT_MAPPER.readValue( str, List.class );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
