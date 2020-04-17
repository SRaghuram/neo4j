/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TransitionsTableTest
{
    private TransitionsTable transitionsTable;

    private TestDatabaseIdRepository databaseIdRepository;

    private TransitionWrapper create;
    private TransitionWrapper start;
    private TransitionWrapper stop;
    private TransitionWrapper drop;
    private TransitionWrapper prepareDrop;
    private TransitionWrapper startAfterStoreCopy;

    @BeforeEach
    void setup()
    {
        databaseIdRepository = new TestDatabaseIdRepository();

        create = new TransitionWrapper( "create", INITIAL, STOPPED, DIRTY );
        start = new TransitionWrapper( "start", STOPPED, STARTED, STOPPED );
        stop = new TransitionWrapper( "stop", STARTED, STOPPED, STOPPED );
        drop = new TransitionWrapper( "drop", STOPPED, DROPPED, DIRTY );
        prepareDrop = new TransitionWrapper( "prepareDrop", STARTED, STARTED, UNKNOWN );
        startAfterStoreCopy = new TransitionWrapper( "startAfterStoreCopy", STORE_COPYING, STARTED, STORE_COPYING );

        transitionsTable = TransitionsTable.builder()
                                           .from( INITIAL ).to( DROPPED ).doNothing()
                                           .from( INITIAL ).to( STOPPED ).doTransitions( create.wrapped )
                                           .from( INITIAL ).to( STARTED ).doTransitions( create.wrapped, start.wrapped )
                                           .from( STOPPED ).to( STARTED ).doTransitions( start.wrapped )
                                           .from( STARTED ).to( STOPPED ).doTransitions( stop.wrapped )
                                           .from( STOPPED ).to( DROPPED ).doTransitions( drop.wrapped )
                                           .from( STARTED ).to( DROPPED ).doTransitions( prepareDrop.wrapped, stop.wrapped, drop.wrapped )
                                           .build();
    }

    @Test
    void transitionLookupsShouldReturnCorrectMappings() throws TransitionFailureException
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id, STARTED );
        var desired = new EnterpriseDatabaseState( id, DROPPED );

        // when
        var lookup = this.transitionsTable
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        for ( var transition : lookup )
        {
            transition.doTransition();
        }

        // then
        prepareDrop.assertCalled( id, 1 );
        stop.assertCalled( id, 1 );
        drop.assertCalled( id, 1 );
    }

    @Test
    void extendedTransitionsShouldReturnCorrectMappings() throws TransitionFailureException
    {
        // given
        var extraTransitions = TransitionsTable.builder()
                                               .from( STORE_COPYING ).to( STARTED ).doTransitions( startAfterStoreCopy.wrapped )
                                               .build();
        var extendedTransitions = this.transitionsTable.extendWith( extraTransitions );

        var id = databaseIdRepository.getRaw( "foo" );
        var currentBase = new EnterpriseDatabaseState( id, STARTED );
        var desiredBase = new EnterpriseDatabaseState( id, DROPPED );
        var currentExtended = new EnterpriseDatabaseState( id, STORE_COPYING );
        var desiredExtended = new EnterpriseDatabaseState( id, STARTED );

        // when
        var lookupBase = extendedTransitions
                .fromCurrent( currentBase )
                .toDesired( desiredBase )
                .collect( Collectors.toList() );

        for ( var prepared : lookupBase )
        {
            prepared.doTransition();
        }

        var lookupExtended = extendedTransitions
                .fromCurrent( currentExtended )
                .toDesired( desiredExtended )
                .collect( Collectors.toList() );

        for ( var transition : lookupExtended )
        {
            transition.doTransition();
        }

        // then
        prepareDrop.assertCalled( id, 1 );
        stop.assertCalled( id, 1 );
        drop.assertCalled( id, 1 );
        startAfterStoreCopy.assertCalled( id, 1 );
    }

    @Test
    void extendedTransitionsShouldOverrideMappings() throws TransitionFailureException
    {
        // given
        var extraTransitions = TransitionsTable.builder()
                                               .from( STARTED ).to( DROPPED ).doTransitions( stop.wrapped, drop.wrapped )
                                               .build();
        var extendedTransitions = this.transitionsTable.extendWith( extraTransitions );

        var id = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id, STARTED );
        var desired = new EnterpriseDatabaseState( id, DROPPED );

        // when
        var lookup = extendedTransitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        for ( var transition : lookup )
        {
            transition.doTransition();
        }

        // then
        prepareDrop.assertCalled( id, 0 );
        stop.assertCalled( id, 1 );
        drop.assertCalled( id, 1 );
    }

    @Test
    void lookupDroppedToAnyShouldThrowForSameDbId()
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id, DROPPED );
        var desired = new EnterpriseDatabaseState( id, STARTED );

        // when then throw
        try
        {
            transitionsTable.fromCurrent( current ).toDesired( desired );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage() ).contains( "'DROPPED', which is a final state" );
        }
    }

    @Test
    void lookupDroppedToAnyForDifferentDbIdsShouldPrepareTransitionsWithCorrectIds() throws TransitionFailureException
    {
        // given
        var id1 = databaseIdRepository.getRaw( "foo" );
        databaseIdRepository.invalidate( id1 );
        var id2 = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id1, DROPPED );
        var desired = new EnterpriseDatabaseState( id2, STARTED );

        // when
        var lookup = transitionsTable
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        for ( var transition : lookup )
        {
            transition.doTransition();
        }

        // then
        create.assertCalled( id2, 1 );
        start.assertCalled( id2, 1 );
    }

    @Test
    void lookupAnyToAnyForDifferentDbIdsShouldPrepareDropTransition() throws TransitionFailureException
    {
        // given
        var id1 = databaseIdRepository.getRaw( "foo" );
        databaseIdRepository.invalidate( id1 );
        var id2 = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id1, STARTED );
        var desired = new EnterpriseDatabaseState( id2, STARTED );

        // when
        var lookup = transitionsTable
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        for ( var transition : lookup )
        {
            transition.doTransition();
        }

        // then
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
        var current = new EnterpriseDatabaseState( id, STARTED );
        var desired = new EnterpriseDatabaseState( id, STORE_COPYING );

        // when then throw
        try
        {
            transitionsTable.fromCurrent( current ).toDesired( desired );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage() ).contains( "unsupported state transition" );
        }
    }

    @Test
    void lookupTransitionToSameStateShouldReturnEmptyStream()
    {
        // given
        var id = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id, STARTED );
        var desired = new EnterpriseDatabaseState( id, STARTED );

        // when
        var lookup = transitionsTable
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        // then
        assertTrue( lookup.isEmpty() );
    }

    @Test
    void invalidSequenceShouldThrow()
    {
        // when
        try
        {
            TransitionsTable.builder()
                            .from( STARTED ).to( DROPPED ).doTransitions( stop.wrapped )
                            .build();
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // then
            assertThat( e.getMessage() ).startsWith( "Chain is invalid, it requires result" );
        }

        // when
        try
        {
            TransitionsTable.builder()
                            .from( STARTED ).to( DROPPED ).doTransitions( drop.wrapped )
                            .build();
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // then
            assertThat( e.getMessage() ).startsWith( "Chain is invalid, transition cannot be chained after" );
        }
    }

    @Test
    void doTransitionExceptionShouldRaiseTransactionFailure()
    {
        // given
        var exception = new RuntimeException();
        var throwingTransition = new TransitionWrapper( "throwing", STARTED, DROPPED, DIRTY )
        {
            @Override
            void perform( NamedDatabaseId namedDatabaseId )
            {
                throw exception;
            }
        };

        var extraTransition = TransitionsTable.builder()
                                              .from( STARTED ).to( DROPPED ).doTransitions( throwingTransition.wrapped )
                                              .build();
        var extendedTransitions = this.transitionsTable.extendWith( extraTransition );

        var id = databaseIdRepository.getRaw( "foo" );
        var current = new EnterpriseDatabaseState( id, STARTED );
        var desired = new EnterpriseDatabaseState( id, DROPPED );

        // when
        var lookup = extendedTransitions
                .fromCurrent( current )
                .toDesired( desired )
                .collect( Collectors.toList() );

        // then
        try
        {
            for ( var transition : lookup )
            {
                transition.doTransition();
            }
            fail();
        }
        catch ( TransitionFailureException f )
        {
            var failed = new EnterpriseDatabaseState( id, DIRTY );

            assertEquals( f.getCause(), exception );
            assertEquals( f.failedState(), failed );
            throwingTransition.assertCalled( id, 0 );
        }
    }

    private static class TransitionWrapper
    {
        String name;
        Transition wrapped;
        List<NamedDatabaseId> forTransitionCalls;

        TransitionWrapper( String name, EnterpriseOperatorState from, EnterpriseOperatorState ifSuccess, EnterpriseOperatorState ifFail )
        {
            this.name = name;
            this.wrapped = Transition.from( from ).doTransition( this::perform ).ifSucceeded( ifSuccess ).ifFailed( ifFail );
            this.forTransitionCalls = new ArrayList<>();
        }

        void perform( NamedDatabaseId namedDatabaseId )
        {
            forTransitionCalls.add( namedDatabaseId );
        }

        void assertCalled( NamedDatabaseId calledFor, long expectedTimes )
        {
            Map<NamedDatabaseId,Long> counts = forTransitionCalls.stream().collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) );
            assertEquals( expectedTimes, counts.getOrDefault( calledFor, 0L ),
                          format( "TransitionFunction '%s' was not called the  expected number of times for the databaseId %s", name, calledFor ) );
        }
    }
}
