/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Map;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.test.server.ha.CommercialServerHelper.createNonPersistentServer;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.rest.MasterInfoService.BASE_PATH;
import static org.neo4j.server.rest.MasterInfoService.IS_MASTER_PATH;
import static org.neo4j.server.rest.MasterInfoService.IS_SLAVE_PATH;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class StandaloneHaInfoFunctionalTest
{
    private static CommercialNeoServer server;

    @Inject
    private TestDirectory target;

    @BeforeEach
    void before() throws IOException
    {
        server = createNonPersistentServer(target.directory());
    }

    @AfterEach
    void after()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    void testHaDiscoveryOnStandaloneReturns403()
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    void testIsMasterOnStandaloneReturns403()
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) + IS_MASTER_PATH );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    void testIsSlaveOnStandaloneReturns403()
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( getBasePath( helper ) + IS_SLAVE_PATH );
        assertEquals( SC_FORBIDDEN, response.getStatus() );
    }

    @Test
    void testDiscoveryListingOnStandaloneDoesNotContainHA() throws Exception
    {
        FunctionalTestHelper helper = new FunctionalTestHelper( server );

        JaxRsResponse response = RestRequest.req().get( helper.managementUri() );

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity() );

       assertEquals( 2, ((Map) map.get( "services" )).size() );
    }

    private static String getBasePath( FunctionalTestHelper helper )
    {
        return helper.managementUri() + "/" + BASE_PATH;
    }
}
