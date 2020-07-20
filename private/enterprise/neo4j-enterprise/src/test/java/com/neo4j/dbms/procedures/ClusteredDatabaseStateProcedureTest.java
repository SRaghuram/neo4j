/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
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
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;

import static com.neo4j.causalclustering.discovery.FakeTopologyService.memberId;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
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
    @Test
    void shouldReturnErrorForFailedMembers() throws ProcedureException
    {
        // given
        var idRepository = new TestDatabaseIdRepository();
        var namedDatabaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( namedDatabaseId ) );

        var successfulState = new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED );
        var failureMessage = "Too many databases";
        var failedState = new DiscoveryDatabaseState( namedDatabaseId.databaseId(), INITIAL, new DatabaseManagementException( failureMessage ) );

        topologyService.setState( cores, successfulState );
        topologyService.setState( replicas, successfulState );
        topologyService.setState( Set.of( memberId( 2 ) ), failedState );

        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService );
        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( namedDatabaseId.name() )}, mock( ResourceTracker.class ) );

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
        var namedDatabaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( namedDatabaseId ) );
        var successfulState = new DiscoveryDatabaseState( namedDatabaseId.databaseId(), STARTED );

        topologyService.setState( cores, successfulState );
        topologyService.setState( replicas, successfulState );

        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService );

        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( namedDatabaseId.name() )}, mock( ResourceTracker.class ) );

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
        var defaultNamedDatabaseId = idRepository.defaultDatabase();
        var cores = FakeTopologyService.memberIds( 0, 3 );
        var replicas = FakeTopologyService.memberIds( 3, 5 );

        var topologyService = new FakeTopologyService( cores, replicas, memberId( 0 ), Set.of( defaultNamedDatabaseId ) );

        var coreOnlyNamedDatabaseId = idRepository.getRaw( "coreOnly" );
        topologyService.setDatabases( cores, Set.of( defaultNamedDatabaseId.databaseId(), coreOnlyNamedDatabaseId.databaseId() ) );

        var defaultDatabaseState = new DiscoveryDatabaseState( defaultNamedDatabaseId.databaseId(), STARTED );
        var coreOnlyDatabaseState = new DiscoveryDatabaseState( coreOnlyNamedDatabaseId.databaseId(), STARTED );

        topologyService.setState( cores, defaultDatabaseState );
        topologyService.setState( replicas, defaultDatabaseState );
        topologyService.setState( cores, coreOnlyDatabaseState );

        var procedure = new ClusteredDatabaseStateProcedure( idRepository, topologyService );

        // when
        var defaultResult = procedure.apply( mock( Context.class ),
                new AnyValue[]{stringValue( defaultNamedDatabaseId.name() )}, mock( ResourceTracker.class ) );
        var coreOnlyResult = procedure.apply( mock( Context.class ),
                new AnyValue[]{stringValue( coreOnlyNamedDatabaseId.name() )}, mock( ResourceTracker.class ) );

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
}
