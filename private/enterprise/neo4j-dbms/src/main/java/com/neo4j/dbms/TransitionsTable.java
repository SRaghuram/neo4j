/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static java.lang.String.format;

/**
 * Instances of this class contain mappings between pairs of database states and the steps needed to take a database from one state to another.
 *
 * The class itself contains numerous other types for building these mappings, or performing lookups against them.
 */
final class TransitionsTable
{
    private final Map<Pair<EnterpriseOperatorState,EnterpriseOperatorState>,Transition[]> transitionsTable;

    private TransitionsTable( Map<Pair<EnterpriseOperatorState,EnterpriseOperatorState>,Transition[]> transitionsTable )
    {
        this.transitionsTable = transitionsTable;
    }

    public static TransitionsBuilder builder()
    {
        return new TransitionsBuilder();
    }

    TransitionLookup fromCurrent( EnterpriseDatabaseState current )
    {
        return new TransitionLookup( current );
    }

    public TransitionsTable extendWith( TransitionsTable other )
    {
        var combined = new HashMap<>( this.transitionsTable );
        combined.putAll( other.transitionsTable );
        return new TransitionsTable( combined );
    }

    private Stream<Transition.Prepared> lookup( TransitionLookup lookup )
    {
        if ( !Objects.equals( lookup.current.databaseId(), lookup.desired.databaseId() ) )
        {
            // If the current and desired databases states have different ids
            //    then the database must have been dropped and needs recreating with it's new id.
            //    This is essentially two lookups for two sets of transition steps, which are then stitched together and returned.
            var dropCurrent = prepareTransitionFunctions( lookup.current.operatorState(), DROPPED, lookup.current.databaseId() );
            var createNext = prepareTransitionFunctions( INITIAL, lookup.desired.operatorState(), lookup.desired.databaseId() );
            return Stream.concat( dropCurrent, createNext );
        }

        return prepareTransitionFunctions( lookup.current.operatorState(), lookup.desired.operatorState(), lookup.current.databaseId() );
    }

    private Stream<Transition.Prepared> prepareTransitionFunctions( EnterpriseOperatorState current, EnterpriseOperatorState desired,
                                                                    NamedDatabaseId namedDatabaseId )
    {
        if ( current == desired )
        {
            return Stream.empty();
        }
        else if ( current == DROPPED )
        {
            throw new IllegalArgumentException( format( "Trying to set database %s to %s, but is 'DROPPED', which is a final state.",
                    namedDatabaseId, desired ) );
        }

        var transitions = transitionsTable.get( Pair.of( current, desired ) );

        if ( transitions == null )
        {
            throw new IllegalArgumentException( format( "%s -> %s is an unsupported state transition", current, desired ) );
        }
        // Before we return a stream of the transition functions needed to go from current to desired
        //    we pre-populate the functions with their databaseId parameter. This could be done at call
        //    time in the reconciler, but we do it here as its easier to provide sub-streams of functions
        //    with different databaseIds this way e.g. for DROP->CREATE transitions.
        return Arrays.stream( transitions ).map( tn -> tn.prepare( namedDatabaseId ) );
    }

    /**
     * Simple step-builder for constructing lookups against the transitions table.
     */
    class TransitionLookup implements NeedsDesired
    {
        private final EnterpriseDatabaseState current;
        private EnterpriseDatabaseState desired;

        private TransitionLookup( EnterpriseDatabaseState current )
        {
            Objects.requireNonNull( current, "You must specify a current state for a transition!" );
            this.current = current;
        }

        @Override
        public Stream<Transition.Prepared> toDesired( EnterpriseDatabaseState desired )
        {
            Objects.requireNonNull( desired, "You must specify a desired state for a transition!" );
            this.desired = desired;
            return lookup( this );
        }
    }

    public interface NeedsDesired
    {
        Stream<Transition.Prepared> toDesired( EnterpriseDatabaseState desired );
    }

    /**
     * Step-builder for constructing terse, readable lookup tables of database state pairs to corresponding streams of actions, required to go between them.
     * The step-builder prevents "syntactic" error in lookup table construction, but no exhaustiveness checking is employed. If a particular transition is
     * not specified, but is later looked up, then an IllegalArgumentException will be thrown.
     */
    public static class TransitionsBuilder implements NeedsFrom, NeedsTo, NeedsDo, BuildOrContinue
    {
        private EnterpriseOperatorState from;
        private EnterpriseOperatorState to;
        private Transition[] transitions;
        private final Map<Pair<EnterpriseOperatorState,EnterpriseOperatorState>,Transition[]> transitionsTable;

        private TransitionsBuilder()
        {
            transitionsTable = new HashMap<>();
        }

        @Override
        public NeedsTo from( EnterpriseOperatorState from )
        {
            storePreviousEntry();
            this.from = from;
            return this;
        }

        @Override
        public NeedsDo to( EnterpriseOperatorState to )
        {
            this.to = to;
            return this;
        }

        @Override
        public BuildOrContinue doTransitions( Transition... transitions )
        {
            Transition.validate( from, to, Arrays.stream( transitions ).iterator() );
            this.transitions = transitions;
            return this;
        }

        @Override
        public BuildOrContinue doNothing()
        {
            // A function to transfer to the desired state with no side effects.
            var noOp = Transition.from( from ).doTransition( ignored -> {} ).ifSucceeded( to ).ifFailed( to );
            this.transitions = new Transition[] { noOp };
            return this;
        }

        @Override
        public TransitionsTable build()
        {
            storePreviousEntry();
            return new TransitionsTable( transitionsTable );
        }

        private void storePreviousEntry()
        {
            if ( from != null && to != null && transitions != null )
            {
                transitionsTable.put( Pair.of( from, to ), transitions );
            }
            else if ( from != null || to != null || transitions != null )
            {
                throw new IllegalStateException( "TransitionFunction builder is only partially complete" );
            }
        }
    }

    /* TransitionsBuilder steps */
    public interface NeedsFrom
    {
        NeedsTo from( EnterpriseOperatorState state );
    }

    public interface NeedsTo
    {
        NeedsDo to( EnterpriseOperatorState state );
    }

    public interface NeedsDo
    {
        BuildOrContinue doTransitions( Transition... transitions );
        BuildOrContinue doNothing();
    }

    public interface BuildOrContinue
    {
        NeedsTo from( EnterpriseOperatorState state );
        TransitionsTable build();
    }

    /* Transition function type and its lazy/supplier wrapper */

    @FunctionalInterface
    public interface PreparedTransition extends Supplier<EnterpriseDatabaseState>
    {
        EnterpriseDatabaseState doTransition();

        @Override
        default EnterpriseDatabaseState get()
        {
            return doTransition();
        }
    }
}
