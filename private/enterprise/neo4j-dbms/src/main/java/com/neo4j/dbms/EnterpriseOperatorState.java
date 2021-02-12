/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.neo4j.dbms.OperatorState;

import static java.util.Comparator.comparingInt;
import static java.util.Comparator.nullsLast;

/**
 * Instances of this type represent the possible states a neo4j operator may "desire" enterprise databases to be in.
 *
 * Instances should posses a total, deterministic ordering relative to *all*
 * other possible OperatorStates.
 *
 * Note that the order of members in this enum are depended upon for Serialization, and therefore should not be changed!
 */
public enum EnterpriseOperatorState implements OperatorState
{
    // States which may be desired by an operator
    STOPPED( "offline", 100 ),
    STORE_COPYING( "store copying", 200 ),
    STARTED( "online", 300 ),
    DROPPED( "dropped", 50, true ),
    DROPPED_DUMPED( "dropped", 25, true ),
    QUARANTINED( "quarantined", 0 ),

    // States which should never be desired by an operator
    UNKNOWN( "unknown", 400 ),
    INITIAL( "initial", 500 ),
    DIRTY( "dirty", 600 );

    private final String description;
    private final int precedence;
    private final boolean terminal;

    EnterpriseOperatorState( String description, int precedence )
    {
        this( description, precedence, false );
    }

    EnterpriseOperatorState( String description, int precedence, boolean terminal )
    {
        this.description = description;
        this.precedence = precedence;
        this.terminal = terminal;
    }

    /**
     * @return human readable name for the desired state.
     */
    @Override
    public String description()
    {
        return description;
    }

    /**
     * @return the precedence of this EnterpriseOperatorState as compared to others. Lower equals higher priority.
     */
    int precedence()
    {
       return precedence;
    }

    @Override
    public boolean terminal()
    {
        return terminal;
    }
    /**
     * Returns whichever of the left or right OperatorStates should take precedence over the other.
     *
     * Note that if the EnterpriseOperatorState parameters are equal, this method defaults to returning the left.
     * Also, null parameters always lose the precedence comparison
     *
     * @param left the left operator state
     * @param right the right operator state
     * @return the state with lower precedence, or left, if the precedence is equal
     */
    static EnterpriseOperatorState minByPrecedence( EnterpriseOperatorState left, EnterpriseOperatorState right )
    {
        var precedenceCompare = nullsLast( comparingInt( EnterpriseOperatorState::precedence ) );
        return precedenceCompare.compare( left, right ) <= 0 ? left : right;
    }

    /**
     * Returns a binary operator for comparing objects by their EnterpriseOperatorState using {@code minByPrecedence}
     * @see EnterpriseDatabaseState
     */
    static <T> BinaryOperator<T> minByOperatorState( Function<T,EnterpriseOperatorState> toOperatorState )
    {
        return ( left, right ) ->
        {
            var leftState = toOperatorState.apply( left );
            var rightState = toOperatorState.apply( right );
            var min = minByPrecedence( leftState, rightState );
            return min == leftState ? left : right;
        };
    }
}
