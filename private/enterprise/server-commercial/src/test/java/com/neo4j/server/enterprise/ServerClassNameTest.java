/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.junit.Test;

import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

import static org.junit.Assert.assertEquals;

/**
 * The classes that extend AbstractNeoServer are currently known to be:
 * CommunityNeoServer and EnterpriseNeoServer
 * <p>
 * This test asserts that those names won't change, for example during an
 * otherwise perfectly reasonable refactoring. Changing those names will cause
 * problems for the server which relies on those names to yield the correct
 * Neo4j edition (community, enterprise) to the Web UI and other clients.
 * <p>
 * Although this est asserts naming against classes in other modules (neo4j),
 * it lives in neo4j-enterprise because otherwise the CommunityNeoServer
 * and EnterpriseNeoServer would not be visible.
 */
public class ServerClassNameTest
{
    @Test
    public void shouldMaintainNamingOfCommunityNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
            throws Exception
    {
        assertEquals( getErrorMessage( CommunityNeoServer.class ), "communityneoserver",
                CommunityNeoServer.class.getSimpleName().toLowerCase() );
    }

    @Test
    public void shouldMaintainNamingOfEnterpriseNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
            throws Exception
    {
        assertEquals( getErrorMessage( OpenEnterpriseNeoServer.class ), "openenterpriseneoserver",
                OpenEnterpriseNeoServer.class.getSimpleName().toLowerCase() );
    }

    @Test
    public void shouldMaintainNamingOfCommercialNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
            throws Exception
    {
        assertEquals( getErrorMessage( CommercialNeoServer.class ), "commercialneoserver",
                CommercialNeoServer.class.getSimpleName().toLowerCase() );
    }

    private String getErrorMessage( Class<? extends AbstractNeoServer> neoServerClass )
    {
        return "The " + neoServerClass.getSimpleName() + " class appears to have been renamed. There is a strict " +
                "dependency from the REST API VersionAndEditionService on the name of that class. If you want " +
                "to change the name of that class, then remember to change VersionAndEditionService, " +
                "VersionAndEditionServiceTest and, of course, this test. ";
    }
}
