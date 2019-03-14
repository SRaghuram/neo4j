/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.CausalClusterInProcessBuilder;
import org.neo4j.harness.PortAuthorityPortPickingStrategy;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.rule.TestDirectory;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CausalClusterStatusEndpointHelpers
{
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
                .withBuilder( EnterpriseInProcessServerBuilder::new )
                .withCores( 3 )
                .withReplicas( 2 )
                .withLogger( LOG_PROVIDER )
                .atPath( clusterDirectory.toPath() )
                .withOptionalPortsStrategy( new PortAuthorityPortPickingStrategy() )
                .build();

        cluster.boot();
        return cluster;
    }

    static ServerControls getLeader( CausalClusterInProcessBuilder.CausalCluster cluster )
    {
        return cluster.getCoreControls()
                .stream()
                .filter( core -> Role.LEADER.equals( ((CoreGraphDatabase) core.graph()).getRole() ) )
                .findAny()
                .orElseThrow( () -> new IllegalStateException( "Leader does not exist" ) );
    }

    static Boolean[] availabilityStatuses( URI server )
    {
        Boolean writable = Boolean.parseBoolean( getStatusRaw( getWritableEndpoint( server ) ) );
        Boolean readonly = Boolean.parseBoolean( getStatusRaw( getReadOnlyEndpoint( server ) ) );
        Boolean availability = Boolean.parseBoolean( getStatusRaw( getAvailability( server ) ) );
        return new Boolean[]{writable, readonly, availability};
    }

    static String getStatusRaw( String address )
    {
        return getStatusRaw( address, null );
    }

    private static String getStatusRaw( String address, Integer expectedStatus )
    {
        Client client = Client.create();
        ClientResponse r = client.resource( address ).accept( APPLICATION_JSON ).get( ClientResponse.class );
        if ( expectedStatus != null )
        {
            assertEquals( expectedStatus.intValue(), r.getStatus() );
        }
        return r.getEntity( String.class );
    }

    static Map<String,Object> getStatus( String address )
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String raw = getStatusRaw( address );
        try
        {
            return objectMapper.readValue( raw, new TypeReference<Map<String,Object>>()
            {
            } );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
