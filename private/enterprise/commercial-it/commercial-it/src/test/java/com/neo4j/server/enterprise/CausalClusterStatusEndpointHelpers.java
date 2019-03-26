/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.harness.internal.CausalClusterInProcessBuilder;
import com.neo4j.harness.internal.CommercialInProcessNeo4jBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.PortAuthorityPortPickingStrategy;
import org.neo4j.harness.junit.Neo4j;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

class CausalClusterStatusEndpointHelpers
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final LogProvider LOG_PROVIDER = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );

    static void writeSomeData( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        GraphDatabaseService db = getLeader( cluster ).graph();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( Label.label( "MyNode" ) );
            tx.success();
        }
    }

    static CausalClusterInProcessBuilder.CausalCluster startCluster( TestDirectory testDirectory ) throws InterruptedException
    {
        File clusterDirectory = testDirectory.directory( "CLUSTER" );
        CausalClusterInProcessBuilder.CausalCluster cluster = CausalClusterInProcessBuilder.init()
                .withBuilder( CommercialInProcessNeo4jBuilder::new )
                .withCores( 3 )
                .withReplicas( 2 )
                .withLogger( LOG_PROVIDER )
                .atPath( clusterDirectory.toPath() )
                .withOptionalPortsStrategy( new PortAuthorityPortPickingStrategy() )
                .build();

        cluster.boot();
        return cluster;
    }

    static Neo4j getLeader( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        return cluster.getCoreNeo4j()
                .stream()
                .filter( core -> Role.LEADER.equals( ((CoreGraphDatabase) core.graph()).getRole() ) )
                .findAny()
                .orElseThrow( () -> new IllegalStateException( "Leader does not exist" ) );
    }

    static Boolean[] availabilityStatuses( URI server )
    {
        boolean writable = Boolean.parseBoolean( getStatusRaw( getWritableEndpoint( server ) ) );
        boolean readonly = Boolean.parseBoolean( getStatusRaw( getReadOnlyEndpoint( server ) ) );
        boolean availability = Boolean.parseBoolean( getStatusRaw( getAvailability( server ) ) );
        return new Boolean[]{writable, readonly, availability};
    }

    static String getStatusRaw( String address )
    {
        var request = HttpRequest.newBuilder( URI.create( address ) )
                .header( ACCEPT, APPLICATION_JSON )
                .GET()
                .build();

        try
        {
            var response = newHttpClient().send( request, ofString() );
            return response.body();
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

    @SuppressWarnings( "unchecked" )
    static Map<String,Object> getStatus( String address )
    {
        try
        {
            String raw = getStatusRaw( address );
            return OBJECT_MAPPER.readValue( raw, Map.class );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static String getReadOnlyEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/read-only" );
    }

    static String getWritableEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/writable" );
    }

    private static String getAvailability( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/available" );
    }

    static String getCcEndpoint( URI server )
    {
        return endpointFromServer( server, "/db/manage/server/causalclustering/status" );
    }

    private static String endpointFromServer( URI server, String endpoint )
    {
        return (server.toString() + endpoint).replaceAll( "//", "/" ).replaceAll( ":/", "://" );
    }
}
