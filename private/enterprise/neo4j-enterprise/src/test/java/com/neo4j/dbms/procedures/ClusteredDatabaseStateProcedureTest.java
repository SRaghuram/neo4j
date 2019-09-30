/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.function.Function.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.stringValue;

class ClusteredDatabaseStateProcedureTest
{
    // Test throw with invalid

    // Test throw when database not found

    // Test return empty error for all databases when no error

    @Test
    void shouldOnlyReturnErrorForFailedMembers() throws ProcedureException
    {
        // given
        var idRepository = new TestDatabaseIdRepository();
        var databaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( databaseId ) );

        var successfulState = new EnterpriseDatabaseState( databaseId, STARTED );
        var failureMessage = "Too many databases";
        var failedState = new EnterpriseDatabaseState( databaseId, EnterpriseOperatorState.INITIAL )
                .failed( new DatabaseManagementException( failureMessage ) );

        topologyService.setState( cores, successfulState );
        topologyService.setState( replicas, successfulState );
        topologyService.setState( Set.of( memberId( 2 ) ), failedState );

        var localStateService = alwaysHealthyLocalStateService();

        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService, localStateService );
        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( databaseId.uuid().toString() )}, mock( ResourceTracker.class ) );

        // then
        var resultRows = Iterators.asList( result );
        var statusErrorColumns = resultRows.stream().map( columns -> Pair.of( columns[2], columns[3] ) )
                .filter( p -> !Objects.equals( p.first(), stringValue("online") ) )
                .collect( Collectors.toList() );

        assertEquals( 1, statusErrorColumns.size(), "Procedure should only return one row which isn't online" );
        assertEquals( 5, resultRows.size(), "Procedure should result one row for each cluster member" );
        var errorMessage = ((StringValue) statusErrorColumns.get( 0 ).other()).stringValue();
        assertThat( "The error column should include the correct failure message", errorMessage, containsString( failureMessage ) );
    }

    @Test
    void shouldCorrectlyReportAllRoles() throws ProcedureException
    {
        // given
        var idRepository = new TestDatabaseIdRepository();
        var databaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( databaseId ) );
        var successfulState = new EnterpriseDatabaseState( databaseId, STARTED );

        topologyService.setState( cores, successfulState );
        topologyService.setState( replicas, successfulState );

        var localStateService = alwaysHealthyLocalStateService();

        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService, localStateService );

        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( databaseId.uuid().toString() )}, mock( ResourceTracker.class ) );

        // then
        var resultRows = Iterators.asList( result );
        var numRoles = resultRows.stream().map( columns -> columns[0] ).collect( Collectors.groupingBy( identity(), Collectors.counting() ) );
        assertEquals( 2, numRoles.getOrDefault( stringValue( "read_replica" ), 0L ) );
        assertEquals( 2, numRoles.getOrDefault( stringValue( "follower" ), 0L ) );
        assertEquals( 1, numRoles.getOrDefault( stringValue( "leader" ), 0L ) );
    }

    @Test
    void shouldOnlyReturnRowsForDatabaseHostingDatabase() throws ProcedureException
    {
        // given
        var idRepository = new TestDatabaseIdRepository();
        var defaultDatabaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( defaultDatabaseId ) );

        var coreOnlyDatabaseId = idRepository.getRaw( "coreOnly" );
        topologyService.setDatabases( cores, Set.of( defaultDatabaseId, coreOnlyDatabaseId ) );

        var defaultDatabaseState = new EnterpriseDatabaseState( defaultDatabaseId, STARTED );
        var coreOnlyDatabaseState = new EnterpriseDatabaseState( coreOnlyDatabaseId, STARTED );

        topologyService.setState( cores, defaultDatabaseState );
        topologyService.setState( replicas, defaultDatabaseState );
        topologyService.setState( cores, coreOnlyDatabaseState );

        var localStateService = alwaysHealthyLocalStateService();

        var localServerInfo = topologyService.allCoreServers().get( memberId( 0 ) );
        var localConfig = TestTopology.configFor( localServerInfo );
        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService, localStateService );

        // when
        var defaultResult = procedure.apply( mock( Context.class ),
                new AnyValue[]{stringValue( defaultDatabaseId.uuid().toString() )}, mock( ResourceTracker.class ) );
        var coreOnlyResult = procedure.apply( mock( Context.class ),
                new AnyValue[]{stringValue( coreOnlyDatabaseId.uuid().toString() )}, mock( ResourceTracker.class ) );

        // then
        assertEquals( 2, countReadReplicas( defaultResult ), "Default Database should be hosted on read replicas" );
        assertEquals( 0, countReadReplicas( coreOnlyResult ), "Core only Database should not be hosted on any read replicas" );
    }

    private long countReadReplicas( RawIterator<AnyValue[],ProcedureException> result ) throws ProcedureException
    {
        var resultRows = Iterators.asList( result );
        return resultRows.stream()
                .map( columns -> columns[0] )
                .filter( role -> Objects.equals( role, stringValue( "read_replica" ) ) )
                .count();
    }

    private DatabaseStateService alwaysHealthyLocalStateService()
    {
        var localStateService = mock( DatabaseStateService.class );
        when( localStateService.stateOfDatabase( any( DatabaseId.class ) ) ).thenReturn( STARTED );
        when( localStateService.causeOfFailure( any( DatabaseId.class ) ) ).thenReturn( Optional.empty() );
        return localStateService;
    }
}
