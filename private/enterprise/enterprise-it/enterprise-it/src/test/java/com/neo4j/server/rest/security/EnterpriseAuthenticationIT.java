/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.rest.security.AuthenticationIT;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static com.neo4j.server.enterprise.helpers.EnterpriseWebContainerBuilder.serverOnRandomPorts;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Override
    public void startServer( boolean authEnabled ) throws IOException
    {
        testWebContainer = serverOnRandomPorts()
                                        .usingDataDir( testDirectory.homeDir().getAbsolutePath() )
                                        .persistent()
                                        .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                                        .build();
    }

    @Test
    public void shouldHavePredefinedRoles() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        String method = "POST";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.security.listRoles()'}]}" );
        HTTP.Response response = HTTP.withBasicAuth( "neo4j", "secret" )
                .request( method, txCommitURL( "system" ), payload );

        // Then
        assertThat(response.status(), equalTo(200));
        ArrayNode errors = (ArrayNode) response.get("errors");
        assertThat( "Should have no errors", errors.size(), equalTo( 0 ) );
        ArrayNode results = (ArrayNode) response.get("results");
        ArrayNode data = (ArrayNode) results.get(0).get("data");
        assertThat( "Should have 6 predefined roles", data.size(), equalTo( 6 ) );
        Stream<String> values = data.findValues( "row" ).stream().map( row -> row.get(0).asText() );
        assertThat( "Expected specific roles", values.collect( Collectors.toList()),
                hasItems( "admin", "architect", "publisher", "editor", "reader", "PUBLIC") );

    }

    @Test
    public void shouldAllowExecutingEnterpriseBuiltInProceduresWithAuthDisabled() throws Exception
    {
        // Given
        startServerWithAuthDisabled();

        // When
        String method = "POST";
        HTTP.RawPayload payload = HTTP.RawPayload.quotedJson(
                "{'statements':[{'statement':'CALL dbms.listQueries()'}]}" );
        HTTP.Response response = HTTP.request( method, txCommitURL(), payload );

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
        testWebContainer = serverOnRandomPorts()
                                        .persistent()
                                        .usingDataDir( testDirectory.homeDir().getAbsolutePath() )
                                        .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( false ) )
                                        .build();
    }
}
