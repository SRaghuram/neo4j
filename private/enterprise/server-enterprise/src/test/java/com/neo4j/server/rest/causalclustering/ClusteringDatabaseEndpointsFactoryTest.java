/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.ws.rs.core.Response;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.factory.DbmsInfo.COMMUNITY;
import static org.neo4j.kernel.impl.factory.DbmsInfo.CORE;
import static org.neo4j.kernel.impl.factory.DbmsInfo.ENTERPRISE;
import static org.neo4j.kernel.impl.factory.DbmsInfo.READ_REPLICA;
import static org.neo4j.kernel.impl.factory.DbmsInfo.UNKNOWN;

class ClusteringDatabaseEndpointsFactoryTest
{
    private static final String KNOWN_DB = "foo";
    private static final String UNKNOWN_DB = "bar";

    @Test
    void shouldBuildStatusForCore()
    {
        var dbService = databaseServiceMock( CORE );
        var databaseStateService = databaseStateServiceStub();

        var status = buildStatus( dbService, KNOWN_DB, databaseStateService );

        assertThat( status, instanceOf( CoreDatabaseEndpoints.class ) );
    }

    @Test
    void shouldBuildStatusForReadReplica()
    {
        var dbService = databaseServiceMock( READ_REPLICA );
        var databaseStateService = databaseStateServiceStub();

        var status = buildStatus( dbService, KNOWN_DB, databaseStateService );

        assertThat( status, instanceOf( ReadReplicaDatabaseEndpoints.class ) );
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
        var databaseStateService = databaseStateServiceStub();

        var status = buildStatus( dbService, UNKNOWN_DB, databaseStateService );

        assertThat( status, instanceOf( FixedResponse.class ) );

        assertEquals( NOT_FOUND.getStatusCode(), status.discover().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.available().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.readonly().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.writable().getStatus() );
        assertEquals( NOT_FOUND.getStatusCode(), status.description().getStatus() );
    }

    @ParameterizedTest( name = "OperatorState {0}" )
    @EnumSource( OperatorStateResponses.class )
    void shouldGiveCorrectResponseForState( OperatorStateResponses stateResponses )
    {
        var dbService = databaseServiceMock( CORE );
        var stateService = new StubDatabaseStateService( id -> new EnterpriseDatabaseState( id, stateResponses.operatorState() ) );

        var status = buildStatus( dbService, KNOWN_DB, stateService );

        stateResponses.assertMatches( status );
    }

    enum OperatorStateResponses
    {
        IS_INITIAL( EnterpriseOperatorState.INITIAL, Response.Status.SERVICE_UNAVAILABLE,
                    new FixedResponseChecker( "Retry-After", "Database " + KNOWN_DB + " is " + EnterpriseOperatorState.INITIAL.description() ) ),
        IS_STARTED( EnterpriseOperatorState.STARTED, Response.Status.OK, null ),
        IS_STOPPED( EnterpriseOperatorState.STOPPED, Response.Status.SERVICE_UNAVAILABLE,
                    new FixedResponseChecker( "Database " + KNOWN_DB + " is " + EnterpriseOperatorState.STOPPED.description() ) ),
        IS_DROPPED( EnterpriseOperatorState.DROPPED, Response.Status.SERVICE_UNAVAILABLE,
                    new FixedResponseChecker( "Database " + KNOWN_DB + " is " + EnterpriseOperatorState.DROPPED.description() ) ),
        IS_STORE_COPYING( EnterpriseOperatorState.STARTED, Response.Status.OK, null );

        private final EnterpriseOperatorState operatorState;
        private final FixedResponseChecker responseEvaluator;
        private final Response.Status statusCode;

        OperatorStateResponses( EnterpriseOperatorState operatorState, Response.Status statusCode, FixedResponseChecker responseEvaluator )
        {
            this.operatorState = operatorState;
            this.statusCode = statusCode;
            this.responseEvaluator = responseEvaluator;
        }

        private EnterpriseOperatorState operatorState()
        {
            return operatorState;
        }

        void assertMatches( ClusteringEndpoints actual )
        {
            if ( responseEvaluator == null )
            {
                assertThat( actual, instanceOf( CoreDatabaseEndpoints.class ) );
            }
            else
            {
                assertThat( actual, instanceOf( FixedResponse.class ) );
                var res = actual.available();

                assertEquals( res, actual.discover() );
                assertEquals( res, actual.readonly() );
                assertEquals( res, actual.writable() );
                assertEquals( res, actual.description() );
                responseEvaluator.assertMatches( (FixedResponse) actual );
            }
            assertEquals( statusCode.getStatusCode(), actual.available().getStatus() );
        }
    }

    static class FixedResponseChecker
    {
        private final Object header;
        private final Object entity;

        FixedResponseChecker( Object entity )
        {
            this( null, entity );
        }

        FixedResponseChecker( Object header, Object entity )
        {
            this.header = header;
            this.entity = entity;
        }

        void assertMatches( FixedResponse fixedResponse )
        {
            var res = fixedResponse.available();

            assertEquals( res, fixedResponse.discover() );
            assertEquals( res, fixedResponse.readonly() );
            assertEquals( res, fixedResponse.writable() );
            assertEquals( res, fixedResponse.description() );

            if ( header != null )
            {
                assertThat( res.getHeaders().keySet().stream().findFirst().get(), equalTo( header ) );
            }
            else
            {
                assertThat( res.getHeaders(), anEmptyMap() );
            }
            assertThat( res.getEntity(), equalTo( entity ) );
        }

        @Override
        public String toString()
        {
            return "{" +
                   "header=" + header +
                   ", entity=" + entity +
                   '}';
        }
    }

    private static void testBuildStatusForStandalone( DbmsInfo standaloneInfo )
    {
        var dbService = databaseServiceMock( standaloneInfo );
        var databaseStateService = databaseStateServiceStub();

        var status = buildStatus( dbService, KNOWN_DB, databaseStateService );

        assertThat( status, instanceOf( FixedResponse.class ) );

        assertEquals( FORBIDDEN.getStatusCode(), status.discover().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.available().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.readonly().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.writable().getStatus() );
        assertEquals( FORBIDDEN.getStatusCode(), status.description().getStatus() );
    }

    private static DatabaseStateService databaseStateServiceStub()
    {
        return new StubDatabaseStateService( id -> new EnterpriseDatabaseState( id, EnterpriseOperatorState.STARTED ) );
    }

    private static ClusteringEndpoints buildStatus( DatabaseManagementService dbService, String databaseName,
                                                   DatabaseStateService databaseStateService )
    {
        return ClusteringDatabaseEndpointsFactory.build( mock( OutputFormat.class ), databaseStateService,
                                                         dbService, databaseName, mock( PerDatabaseService.class ) );
    }

    private static DatabaseManagementService databaseServiceMock( DbmsInfo knownDbmsInfo )
    {
        var dbService = mock( DatabaseManagementService.class );
        var db = mock( GraphDatabaseFacade.class );
        when( db.dbmsInfo() ).thenReturn( knownDbmsInfo );
        var namedDatabaseId = mock( NamedDatabaseId.class );
        when( db.databaseId() ).thenReturn( namedDatabaseId );
        var dependencyResolver = mock( DependencyResolver.class );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dbService.database( KNOWN_DB ) ).thenReturn( db );
        when( dbService.database( UNKNOWN_DB ) ).thenThrow( new DatabaseNotFoundException() );
        when( db.isAvailable( anyLong() ) ).thenReturn( true );
        return dbService;
    }
}
