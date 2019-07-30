/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.After;
import org.junit.Test;

import org.neo4j.kernel.internal.Version;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static com.neo4j.server.enterprise.helpers.CommercialServerBuilder.serverOnRandomPorts;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/*
Note that when running this test from within an IDE, the version field will be an empty string. This is because the
code that generates the version identifier is written by Maven as part of the build process(!). The tests will pass
both in the IDE (where the empty string will be correctly compared).
*/
public class CommercialDiscoveryServiceIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void shouldReportEnterpriseEdition() throws Exception
    {
        // Given
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .persistent()
                .build();
        server.start();
        String releaseVersion = Version.getKernel().getReleaseVersion();

        // When
        HTTP.Response res = HTTP.GET( server.baseUri().toASCIIString() );

        // Then
        assertEquals( 200, res.status() );
        assertThat( res.get( "neo4j_edition" ).asText(), equalTo( "enterprise" ) );
        assertThat( res.get( "neo4j_version" ).asText(), equalTo( releaseVersion ) );
    }

    @Test
    public void shouldReportManagementApi() throws Exception
    {
        // When
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .persistent()
                .build();
        server.start();

        HTTP.Response res = HTTP.GET( server.baseUri().toASCIIString() );

        // Then
        assertEquals( 200, res.status() );
        assertThat( res.get( "management" ).asText(), containsString( "/db/manage" ) );
    }

    @Test
    public void shouldAllowResetManagementApi() throws Exception
    {
        // When
        var uri = "a/different/manage/uri";
        server = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .withRelativeManagementApiUriPath( uri )
                .persistent()
                .build();
        server.start();

        HTTP.Response res = HTTP.GET( server.baseUri().toASCIIString() );

        // Then
        assertEquals( 200, res.status() );
        assertThat( res.get( "management" ).asText(), containsString( uri ) );
    }

}
