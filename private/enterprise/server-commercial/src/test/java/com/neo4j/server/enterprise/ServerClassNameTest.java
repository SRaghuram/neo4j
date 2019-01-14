/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.junit.jupiter.api.Test;

import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerClassNameTest
{
    @Test
    void shouldMaintainNamingOfCommunityNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
    {
        assertEquals( "communityneoserver", CommunityNeoServer.class.getSimpleName().toLowerCase(), getErrorMessage( CommunityNeoServer.class ) );
    }

    @Test
    void shouldMaintainNamingOfCommercialNeoServerSoThatTheNeo4jEditionIsCorrectlyShownToRESTAPICallers()
    {
        assertEquals( "commercialneoserver", CommercialNeoServer.class.getSimpleName().toLowerCase(), getErrorMessage( CommercialNeoServer.class ) );
    }

    private static String getErrorMessage( Class<? extends AbstractNeoServer> neoServerClass )
    {
        return "The " + neoServerClass.getSimpleName() + " class appears to have been renamed. There is a strict " +
                "dependency from the REST API VersionAndEditionService on the name of that class. If you want " +
                "to change the name of that class, then remember to change VersionAndEditionService, " +
                "VersionAndEditionServiceTest and, of course, this test. ";
    }
}
