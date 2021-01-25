/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.member;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultServerSnapshotTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final CoreServerIdentity identityModule = new InMemoryCoreServerIdentity();

    private static Set<OperatorState> discoverableStates()
    {
        return DefaultServerSnapshot.DISCOVERABLE_DATABASE_STATES;
    }

    private static Set<OperatorState> undiscoverableStates()
    {
        return Arrays.stream( EnterpriseOperatorState.values() )
                     .filter( not( discoverableStates()::contains ) )
                     .collect( Collectors.toSet() );
    }

    Map<NamedDatabaseId,DatabaseState> databaseStates( Set<OperatorState> operatorStates, int offset )
    {
        var stateArr = operatorStates.toArray( OperatorState[]::new );
        return IntStream.range( 0, stateArr.length ).mapToObj( i ->
        {
            var state = (EnterpriseOperatorState) stateArr[i];
            var databaseId = databaseIdRepository.getRaw( Integer.toString( i + offset ) );
            return new EnterpriseDatabaseState( databaseId, state );
        } ).collect( Collectors.toMap( EnterpriseDatabaseState::databaseId, Function.identity() ) );
    }

    @Test
    void shouldCorrectlyFilterUndiscoverableDatabases()
    {
        var undiscoverableDatabaseStates = databaseStates( undiscoverableStates(), 0 );
        var offset = undiscoverableDatabaseStates.size();
        var discoverableDatabaseStates = databaseStates( discoverableStates(), offset );
        var allStates = new HashMap<NamedDatabaseId,DatabaseState>();
        allStates.putAll( undiscoverableDatabaseStates );
        allStates.putAll( discoverableDatabaseStates );

        var databaseStates = new StubDatabaseStateService( allStates, EnterpriseDatabaseState::unknown );
        var coreSnapshot = DefaultServerSnapshot.coreSnapshot( identityModule, databaseStates, Map.of() );

        var expected = discoverableDatabaseStates.keySet().stream()
                                                 .map( NamedDatabaseId::databaseId )
                                                 .collect( Collectors.toSet() );
        assertEquals( expected, coreSnapshot.discoverableDatabases() );
    }

    @Test
    void serverSnapshotContentsShouldBeUnmodifiable()
    {
        var databaseId1 = databaseIdRepository.getRaw( "one" );
        var databaseId2 = databaseIdRepository.getRaw( "two" );
        var databaseStates = new StubDatabaseStateService( EnterpriseDatabaseState::unknown );
        var coreSnapshot = DefaultServerSnapshot.coreSnapshot( identityModule, databaseStates, Map.of() );

        assertSame( coreSnapshot.databaseStates(), coreSnapshot.databaseStates() );
        assertThat( coreSnapshot.databaseStates() ).isEmpty();
        assertThrows( UnsupportedOperationException.class,
                      () -> coreSnapshot.databaseLeaderships().put( databaseId2.databaseId(), LeaderInfo.INITIAL ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> coreSnapshot.databaseMemberships().put( databaseId2.databaseId(), identityModule.raftMemberId( databaseId2.databaseId() ) ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> coreSnapshot.databaseStates().put( databaseId2.databaseId(), new EnterpriseDatabaseState( databaseId2, STARTED ) ) );
        assertThrows( UnsupportedOperationException.class,
                      () -> coreSnapshot.discoverableDatabases().add( databaseId1.databaseId() ) );
    }
}
