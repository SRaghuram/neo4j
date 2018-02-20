/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.security;

import org.codehaus.jackson.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    @Override
    public void startServer( boolean authEnabled ) throws IOException
    {
        server = CommercialServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .build();
        server.start();
    }

    @Test
    public void shouldHavePredefinedRoles() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        String method = "POST";
        String path = "db/data/transaction/commit";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.security.listRoles()'}]}" );
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "secret" ) )
                .request( method, server.baseUri().resolve( path ).toString(), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should have 5 predefined roles", data.size(), equalTo( 5 ) );
        Stream<String> values = data.findValues( "row" ).stream().map( row -> row.get(0).asText() );
        assertThat( "Expected specific roles", values.collect( Collectors.toList()),
                hasItems( "admin", "architect", "publisher", "editor", "reader") );

    }

    @Test
    public void shouldAllowExecutingEnterpriseBuiltInProceduresWithAuthDisabled() throws Exception
    {
        // Given
        startServerWithAuthDisabled();

        // When
        String method = "POST";
        String path = "db/data/transaction/commit";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.listQueries()'}]}" );
        HTTP.Response response = HTTP.request( method, server.baseUri().resolve( path ).toString(), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should see our own query", data.size(), equalTo( 1 ) );
    }

    private void startServerWithAuthDisabled() throws IOException
    {
        server = CommercialServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( false ) )
                .build();
        server.start();
    }
}
