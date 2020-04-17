/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.util.VisibleForTesting;

public class Transition
{
    private final Set<EnterpriseOperatorState> validStartStates;
    private final EnterpriseOperatorState successfulEndState;
    private final EnterpriseOperatorState failedEndState;
    private final Consumer<NamedDatabaseId> transitionFunction;

    @VisibleForTesting
    Transition( Set<EnterpriseOperatorState> validStartStates, EnterpriseOperatorState successfulEndState,
                        EnterpriseOperatorState failedEndState, Consumer<NamedDatabaseId> transitionFunction )
    {
        this.validStartStates = validStartStates;
        this.successfulEndState = successfulEndState;
        this.failedEndState = failedEndState;
        this.transitionFunction = transitionFunction;
    }

    static NeedsDo from( EnterpriseOperatorState... validStarts )
    {
        return new StepBuilder( validStarts );
    }

    /**
     * Make sure for each element in sequence, start states contain the successful end state of previous element.
     * Also, the start states of the first step should contain the state specified by the from param, whilst the
     * successful end state of last step should be equal to that specified by the to param.
     *
     * @throws IllegalArgumentException if sequence violates described rules
     */
    static void validate( EnterpriseOperatorState from, EnterpriseOperatorState to, Iterator<Transition> sequence )
    {
        var previous = from;

        while ( sequence.hasNext() )
        {
            var next = sequence.next();
            if ( !next.validStartStates.contains( previous )  )
            {
                throw new IllegalArgumentException( String.format( "Chain is invalid, transition cannot be chained after %s, it requires start state of %s",
                                                                   previous, next.validStartStates ) );
            }

            previous = next.successfulEndState;
        }

        if ( previous != to )
        {
            throw new IllegalArgumentException( String.format( "Chain is invalid, it requires result %s, last transition gives %s", to, from ) );
        }
    }

    Prepared prepare( NamedDatabaseId namedDatabaseId )
    {
        return new Prepared( namedDatabaseId, this );
    }

    static class Prepared
    {

        private NamedDatabaseId namedDatabaseId;
        private Transition transition;

        Prepared( NamedDatabaseId namedDatabaseId, Transition transition )
        {
            this.namedDatabaseId = namedDatabaseId;
            this.transition = transition;
        }

        EnterpriseOperatorState successfulEndState()
        {
            return transition.successfulEndState;
        }

        EnterpriseOperatorState failedEndState()
        {
            return transition.failedEndState;
        }

        EnterpriseDatabaseState doTransition() throws TransitionFailureException
        {
            try
            {
                transition.transitionFunction.accept( namedDatabaseId );
                return new EnterpriseDatabaseState( namedDatabaseId, successfulEndState() );
            }
            catch ( Throwable t )
            {
                var failedState = new EnterpriseDatabaseState( namedDatabaseId, failedEndState() );
                throw new TransitionFailureException( t, failedState );
            }
        }
    }

    // Builder
    private static class StepBuilder implements NeedsEndSuccess, NeedsDo, NeedsEndFail
    {
        private Set<EnterpriseOperatorState> validStarts;
        private EnterpriseOperatorState endSuccess;
        private Consumer<NamedDatabaseId> transitionFunction;

        private StepBuilder( EnterpriseOperatorState... validStarts )
        {
            this.validStarts = Set.of( validStarts );
        }

        @Override
        public NeedsEndSuccess doTransition( Consumer<NamedDatabaseId> transitionFunction )
        {
            this.transitionFunction = transitionFunction;
            return this;
        }

        @Override
        public NeedsEndFail ifSucceeded( EnterpriseOperatorState end )
        {
            this.endSuccess = end;
            return this;
        }

        @Override
        public Transition ifFailed( EnterpriseOperatorState endFail )
        {
            return new Transition( validStarts, endSuccess, endFail, transitionFunction );
        }
    }

    // Build steps
    interface NeedsDo
    {
        NeedsEndSuccess doTransition( Consumer<NamedDatabaseId> transitionFunction );
    }

    interface NeedsEndSuccess
    {
        NeedsEndFail ifSucceeded( EnterpriseOperatorState end );
    }

    interface NeedsEndFail
    {
        Transition ifFailed( EnterpriseOperatorState endFail );
    }
}
