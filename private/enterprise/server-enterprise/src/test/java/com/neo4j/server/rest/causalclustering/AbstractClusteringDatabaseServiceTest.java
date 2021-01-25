/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;

import org.neo4j.dbms.api.DatabaseManagementService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractClusteringDatabaseServiceTest
{
    @Test
    void shouldReturnServerErrorOnUnknownException()
    {
        RuntimeException exception = new RuntimeException( "foo" );

        var databaseService = mock( DatabaseManagementService.class );
        when( databaseService.database( anyString() ) ).thenThrow( exception );

        var causalClusteringServiceStub = new CausalClusteringServiceStub( databaseService );

        Response responseStatus = causalClusteringServiceStub.discover();
        assertEquals( responseStatus.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode() );
        assertEquals( responseStatus.getEntity().toString(), exception.toString() );
    }

    static class CausalClusteringServiceStub extends AbstractClusteringDatabaseService
    {
        CausalClusteringServiceStub( DatabaseManagementService dbService )
        {
            super( null, null, dbService, "foo" );
        }

        @Override
        public String relativePath( String databaseName )
        {
            throw new UnsupportedOperationException();
        }
    }
}
