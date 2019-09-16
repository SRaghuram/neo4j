/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.junit.jupiter.api.Test;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMUNITY;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.CORE;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.ENTERPRISE;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.READ_REPLICA;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.UNKNOWN;

class CausalClusteringStatusFactoryTest
{
    private static final String KNOWN_DB = "foo";
    private static final String UNKNOWN_DB = "bar";

    @Test
    void shouldBuildStatusForCore()
    {
        var dbService = databaseServiceMock( CORE );

        var status = buildStatus( dbService, KNOWN_DB );

        assertThat( status, instanceOf( CoreStatus.class ) );
    }

    @Test
    void shouldBuildStatusForReadReplica()
    {
        var dbService = databaseServiceMock( READ_REPLICA );

        var status = buildStatus( dbService, KNOWN_DB );

        assertThat( status, instanceOf( ReadReplicaStatus.class ) );
    }

    @Test
    void shouldBuildStatusForCommunityStandalone()
    {
        testBuildStatusForStandalone( COMMUNITY );
    }

    @Test
    void shouldBuildStatusForEnterpriseStandalone()
    {
        testBuildStatusForStandalone( ENTERPRISE );
    }

    @Test
    void shouldBuildStatusForUnknownDatabase()
    {
        var dbService = databaseServiceMock( UNKNOWN );

        var status = buildStatus( dbService, UNKNOWN_DB );

        assertThat( status, instanceOf( FixedStatus.class ) );

        assertEquals( NOT_FOUND.getStatusCode(), status.discover().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.available().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.readonly().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.writable().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.description().getStatus() );
    }

    private static void testBuildStatusForStandalone( DatabaseInfo standaloneInfo )
    {
        var dbService = databaseServiceMock( standaloneInfo );

        var status = buildStatus( dbService, KNOWN_DB );

        assertThat( status, instanceOf( FixedStatus.class ) );

        assertEquals( FORBIDDEN.getStatusCode(), status.discover().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.available().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.readonly().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.writable().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.description().getStatus() );
    }

    private static CausalClusteringStatus buildStatus( DatabaseService dbService, String databaseName )
    {
        return CausalClusteringStatusFactory.build( mock( OutputFormat.class ), dbService, databaseName, mock( ClusterService.class ) );
    }

    private static DatabaseService databaseServiceMock( DatabaseInfo knownDbInfo )
    {
        var dbService = mock( DatabaseService.class );
        var db = mock( GraphDatabaseFacade.class );
        var dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( DatabaseInfo.class ) ).thenReturn( knownDbInfo );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dbService.getDatabase( KNOWN_DB ) ).thenReturn( db );
        when( dbService.getDatabase( UNKNOWN_DB ) ).thenThrow( new DatabaseNotFoundException() );
        return dbService;
    }
}
