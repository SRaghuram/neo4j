/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.rest.causalclustering.ClusteringDatabaseService;
import org.junit.After;
import org.junit.Test;

import org.neo4j.kernel.internal.Version;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

import static com.neo4j.server.enterprise.helpers.EnterpriseWebContainerBuilder.serverOnRandomPorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/*
Note that when running this test from within an IDE, the version field will be an empty string. This is because the
code that generates the version identifier is written by Maven as part of the build process(!). The tests will pass
both in the IDE (where the empty string will be correctly compared).
*/
public class EnterpriseDiscoveryServiceIT extends ExclusiveWebContainerTestBase
{
    private TestWebContainer testWebContainer;

    @After
    public void stopTheServer()
    {
        testWebContainer.shutdown();
    }

    @Test
    public void shouldReportEnterpriseEdition() throws Exception
    {
        // Given
        testWebContainer = serverOnRandomPorts()
                .usingDataDir( folder.directory( name.getMethodName() ).toAbsolutePath().toString() )
                .persistent()
                .build();
        String releaseVersion = Version.getKernel().getReleaseVersion();

        // When
        HTTP.Response res = HTTP.GET( testWebContainer.getBaseUri().toASCIIString() );

        // Then
        assertEquals( 200, res.status() );
        assertThat( res.get( "neo4j_edition" ).asText() ).isEqualTo( "enterprise" );
        assertThat( res.get( "neo4j_version" ).asText() ).isEqualTo( releaseVersion );

        assertThat( res.get( ClusteringDatabaseService.KEY ) ).isNull(); // no CC URI in standalone
    }
}
