/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.RESTRequestGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class UsersIT extends ExclusiveServerTestBase
{
    @Rule
    public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );
    private CommunityNeoServer server;

    @Test
    @Documented( "User status\n" +
                 "\n" +
                 "Given that you know the current password, you can ask the server for the user status." )
    public void user_status() throws JsonParseException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader( "neo4j", "secret" ) )
                .get( userURL( "neo4j" ) );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat( data.get( "username" ).asText(), equalTo( "neo4j" ) );
        assertThat( data.get( "password_change_required" ).asBoolean(), equalTo( false ) );
        assertThat( data.get( "password_change" ).asText(), equalTo( passwordURL( "neo4j" ) ) );
    }

    @Test
    @Documented( "User status on first access\n" +
                 "\n" +
                 "On first access, and using the default password, the user status will indicate " +
                 "that the users password requires changing." )
    public void user_status_first_access() throws JsonParseException, IOException
    {
        // Given
        startServer( true );

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader( "neo4j", "neo4j" ) )
                .get( userURL( "neo4j" ) );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat( data.get( "username" ).asText(), equalTo( "neo4j" ) );
        assertThat( data.get( "password_change_required" ).asBoolean(), equalTo( true ) );
        assertThat( data.get( "password_change" ).asText(), equalTo( passwordURL( "neo4j" ) ) );
    }

    @Test
    @Documented( "Changing the user password\n" +
                 "\n" +
                 "Given that you know the current password, you can ask the server to change a users password. " +
                 "You can choose any\n" +
                 "password you like, as long as it is different from the current password." )
    public void change_password() throws IOException
    {
        // Given
        startServer( true );

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 204 )
                .withHeader( HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader( "neo4j", "neo4j" ) )
                .payload( quotedJson( "{'password':'secret'}" ) )
                .post( server.baseUri().resolve( "/user/neo4j/password" ).toString() );

        // Then the new password should work
        assertEquals( 200, HTTP.withBasicAuth( "neo4j", "secret" ).POST( cypherURL(), simpleCypherRequestBody() ).status() );

        // Then the old password should be invalid
        assertEquals( 401, HTTP.withBasicAuth( "neo4j", "neo4j" ).POST( cypherURL(), simpleCypherRequestBody() ).status() );
    }

    @Test
    public void cantChangeToCurrentPassword() throws Exception
    {
        // Given
        startServer( true );

        // When
        HTTP.Response res = HTTP.withBasicAuth( "neo4j", "neo4j" ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'neo4j'}" ) );

        // Then
        assertThat( res.status(), equalTo( 422 ) );
    }

    @After
    public void cleanup()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    public void startServer( boolean authEnabled ) throws IOException
    {
        server = CommunityServerBuilder.serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .build();
        server.start();
    }

    public void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response post = HTTP.withBasicAuth( "neo4j", "neo4j" ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'secret'}" )
        );
        assertEquals( 204, post.status() );
    }

    private String cypherURL()
    {
        return server.baseUri().resolve( "db/neo4j/tx/commit" ).toString();
    }

    private HTTP.RawPayload simpleCypherRequestBody()
    {
        return rawPayload( "{\"statements\": [{\"statement\": \"MATCH (n:MyLabel) RETURN n\"}]}" );
    }

    private String userURL( String username )
    {
        return server.baseUri().resolve( "user/" + username ).toString();
    }

    private String passwordURL( String username )
    {
        return server.baseUri().resolve( "user/" + username + "/password" ).toString();
    }

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }
}
