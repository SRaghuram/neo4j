/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import org.neo4j.server.database.DatabaseService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractCausalClusteringServiceTest
{
    @Test
    void shouldReturnServerErrorOnUnknownException()
    {
        RuntimeException exception = new RuntimeException( "foo" );

        var databaseService = mock( DatabaseService.class );
        when( databaseService.getDatabase( anyString() ) ).thenThrow( exception );

        var causalClusteringServiceStub = new CausalClusteringServiceStub( databaseService );

        Response responseStatus = causalClusteringServiceStub.discover();
        assertEquals( responseStatus.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode() );
        assertEquals( responseStatus.getEntity().toString(), exception.toString() );
    }

    static class CausalClusteringServiceStub extends AbstractCausalClusteringService
    {
        CausalClusteringServiceStub( DatabaseService dbService )
        {
            super( null, null, dbService, "foo" );
        }

        @Override
        public String relativeClusterPath( String databaseName )
        {
            throw new UnsupportedOperationException();
        }
    }
}
