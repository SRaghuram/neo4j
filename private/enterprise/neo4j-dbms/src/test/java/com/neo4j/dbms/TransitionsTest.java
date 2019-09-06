/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.Transitions.Transition;
import com.neo4j.dbms.Transitions.TransitionFunction;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static com.neo4j.dbms.OperatorState.STORE_COPYING;
import static com.neo4j.dbms.OperatorState.INITIAL;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransitionsTest
{
    private Transitions transitions;

    private TestDatabaseIdRepository databaseIdRepository;

    private StubTransitionFunction create;
    private StubTransitionFunction start;
    private StubTransitionFunction stop;
    private StubTransitionFunction drop;
    private StubTransitionFunction prepareDrop;
    private StubTransitionFunction stopBeforeStoreCopy;
    private StubTransitionFunction startAfterStoreCopy;

    @BeforeEach
    void setup()
    {
        databaseIdRepository = new TestDatabaseIdRepository();
        create = new StubTransitionFunction( "create" );
        start = new StubTransitionFunction( "start" );
        stop = new StubTransitionFunction( "stop" );
        drop = new StubTransitionFunction( "drop" );
        prepareDrop = new StubTransitionFunction( "prepareDrop" );
        stopBeforeStoreCopy = new StubTransitionFunction( "stopBeforeStoreCopy" );
        startAfterStoreCopy = new StubTransitionFunction( "startAfterStoreCopy" );

        transitions = Transitions.builder()
                .from( INITIAL ).to( DROPPED ).doTransitions( drop )
                .from( INITIAL ).to( STOPPED ).doTransitions( create )
                .from( INITIAL ).to( STARTED ).doTransitions( create, start )
                .from( STOPPED ).to( STARTED ).doTransitions( start )
                .from( STARTED ).to( STOPPED ).doTransitions( stop )
                .from( STOPPED ).to( DROPPED ).doTransitions( drop )
                .from( STARTED ).to( DROPPED ).doTransitions( prepareDrop, stop, drop )
                .build();
    }

    @Test
    void transitionLookupsShouldReturnCorrectMappings()
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id, STARTED );
        var desired = new DatabaseState( id, DROPPED );

        // when
        var lookup = this.transitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        var expected = Stream.of( prepareDrop, stop, drop )
                .map( tn -> tn.prepare( id ) )
                .collect( Collectors.toList() );

        // then
        assertEquals( expected, lookup );
    }

    @Test
    void extendedTransitionsShouldReturnCorrectMappings()
    {
        // given
        var extraTransitions = Transitions.builder()
                .from( STORE_COPYING ).to( STARTED ).doTransitions( startAfterStoreCopy )
                .build();
        var extendedTransitions = this.transitions.extendWith( extraTransitions );

        var id = databaseIdRepository.getRaw( "foo" );
        var currentBase = new DatabaseState( id, STARTED );
        var desiredBase = new DatabaseState( id, DROPPED );
        var currentExtended = new DatabaseState( id, STORE_COPYING );
        var desiredExtended = new DatabaseState( id, STARTED );

        // when
        var lookupBase = extendedTransitions
                .fromCurrent( currentBase )
                .toDesired( desiredBase )
                .collect( Collectors.toList() );

        var expectedBase = Stream.of( prepareDrop, stop, drop )
                .map( tn -> tn.prepare( id ) )
                .collect( Collectors.toList() );

        var lookupExtended = extendedTransitions
                .fromCurrent( currentExtended )
                .toDesired( desiredExtended )
                .collect( Collectors.toList() );

        var expectedExtended = Stream.of( startAfterStoreCopy )
                .map( tn -> tn.prepare( id ) )
                .collect( Collectors.toList() );

        // then
        assertEquals( expectedBase, lookupBase );
        assertEquals( expectedExtended, lookupExtended );
    }

    @Test
    void extendedTransitionsShouldOverrideMappings()
    {
        // given
        var extraTransitions = Transitions.builder()
                .from( STARTED ).to( DROPPED ).doTransitions( stop, drop )
                .build();
        var extendedTransitions = this.transitions.extendWith( extraTransitions );

        var id = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id, STARTED );
        var desired = new DatabaseState( id, DROPPED );

        // when
        var lookup = extendedTransitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        var expected = Stream.of( stop, drop )
                .map( tn -> tn.prepare( id ) )
                .collect( Collectors.toList() );

        // then
        assertEquals( expected, lookup );
    }

    @Test
    void lookupDroppedToAnyShouldThrowForSameDbId()
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id, DROPPED );
        var desired = new DatabaseState( id, STARTED );

        // when then throw
        try
        {
            transitions.fromCurrent( current ).toDesired( desired );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), Matchers.containsString( "'DROPPED', which is a final state" ) );
        }
    }

    @Test
    void lookupDroppedToAnyForDifferentDbIdsShouldPrepareTransitionsWithCorrectIds()
    {
        // given
        var id1 = databaseIdRepository.getRaw( "foo" );
        databaseIdRepository.invalidate( id1 );
        var id2 = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id1, DROPPED );
        var desired = new DatabaseState( id2, STARTED );

        // when
        var lookup = transitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        lookup.forEach( Transition::doTransition );

        var expected = Stream.of( create, start )
                .map( tn -> tn.prepare( id2 ) )
                .collect( Collectors.toList() );

        // then
        assertEquals( expected, lookup );
        create.assertCalled( id2, 1 );
        start.assertCalled( id2, 1 );
    }

    @Test
    void lookupAnyToAnyForDifferentDbIdsShouldPrepareDropTransition()
    {
        // given
        var id1 = databaseIdRepository.getRaw( "foo" );
        databaseIdRepository.invalidate( id1 );
        var id2 = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id1, STARTED );
        var desired = new DatabaseState( id2, STARTED );

        // when
        var lookup = transitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        lookup.forEach( Transition::doTransition );

        var expectedFirst = Stream.of( prepareDrop, stop, drop ) .map( tn -> tn.prepare( id1 ) );
        var expectedSecond = Stream.of( create, start ) .map( tn -> tn.prepare( id2 ) );

        var expected = Stream.concat( expectedFirst, expectedSecond ).collect( Collectors.toList() );

        // then
        assertEquals( expected, lookup );
        prepareDrop.assertCalled( id1, 1 );
        stop.assertCalled( id1, 1 );
        drop.assertCalled( id1, 1 );
        create.assertCalled( id2, 1 );
        start.assertCalled( id2, 1 );
    }

    @Test
    void lookupNonExistentTransitionShouldThrow()
    {

        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id, STARTED );
        var desired = new DatabaseState( id, STORE_COPYING );

        // when then throw
        try
        {
            transitions.fromCurrent( current ).toDesired( desired );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), Matchers.containsString( "unsupported state transition" ) );
        }
    }

    @Test
    void lookupTransitionToSameStateShouldReturnEmptyStream()
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new DatabaseState( id, STARTED );
        var desired = new DatabaseState( id, STARTED );

        // when
        var lookup = transitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        // then
        assertTrue( lookup.isEmpty() );
    }

    private class StubTransitionFunction implements TransitionFunction
    {
        private String name;
        private List<DatabaseId> forTransitionCalls;

        StubTransitionFunction( String name )
        {
            this.name = name;
            forTransitionCalls = new ArrayList<>();
        }

        @Override
        public DatabaseState forTransition( DatabaseId databaseId )
        {
            forTransitionCalls.add( databaseId );
            return null;
        }

        @Override
        public Transition prepare( DatabaseId databaseId )
        {
            return new StubTransition( name, databaseId, this );
        }

        private void assertCalled( DatabaseId calledFor, long expectedTimes )
        {
            Map<DatabaseId,Long> counts = forTransitionCalls.stream().collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) );
            assertEquals( expectedTimes, counts.getOrDefault( calledFor, 0L ), format( "TransitionFunction#forTransition was not called the " +
                    "expected number of times for the databaseId %s", calledFor ) );
        }
    }

    private class StubTransition implements Transition
    {
        private String name;
        private DatabaseId databaseId;
        private TransitionFunction function;

        StubTransition( String name, DatabaseId databaseId, TransitionFunction function )
        {
            this.name = name;
            this.databaseId = databaseId;
            this.function = function;
        }

        @Override
        public DatabaseState doTransition()
        {
            return function.forTransition( databaseId );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            StubTransition that = (StubTransition) o;
            return Objects.equals( name, that.name ) && Objects.equals( databaseId, that.databaseId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( name, databaseId );
        }
    }
}
