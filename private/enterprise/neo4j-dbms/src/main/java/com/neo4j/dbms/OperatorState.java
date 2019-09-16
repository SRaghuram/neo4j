/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.function.BinaryOperator;
import java.util.function.Function;

import static java.util.Comparator.comparingInt;
import static java.util.Comparator.nullsLast;

/**
 * Instances of this type represent the possible desired states of enterprise Databases.
 *
 * Instances should posses a total, deterministic ordering relative to *all*
 * other possible OperatorStates.
 */
public enum OperatorState
{
    STOPPED( "stopped", 100 ),
    STORE_COPYING( "store_copying", 200 ),
    STARTED( "started", 300 ),
    DROPPED( "dropped", 0 ),
    UNKNOWN( "unknown", 400 ),
    INITIAL( "initial", 500 );

    private final String description;
    private final int precedence;

    OperatorState( String description, int precedence )
    {
        this.description = description;
        this.precedence = precedence;
    }

    /**
     * @return human readable name for the desired state.
     */
    public String description()
    {
        return description;
    }

    /**
     * @return the precedence of this OperatorState as compared to others. Lower equals higher priority.
     */
    int precedence()
    {
       return precedence;
    }

    /**
     * Returns whichever of the left or right OperatorStates should take precedence over the other.
     *
     * Note that if the OperatorState parameters are equal, this method defaults to returning the left.
     * Also, null parameters always lose the precedence comparison
     *
     * @param left the left operator state
     * @param right the right operator state
     * @return the state with lower precedence, or left, if the precedence is equal
     */
    static OperatorState minByPrecedence( OperatorState left, OperatorState right )
    {
        var precedenceCompare = nullsLast( comparingInt( OperatorState::precedence ) );
        return precedenceCompare.compare( left, right ) <= 0 ? left : right;
    }

    /**
     * Returns a binary operator for comparing objects by their OperatorState using {@code minByPrecedence}
     * @see DatabaseState
     */
    static <T> BinaryOperator<T> minByOperatorState( Function<T,OperatorState> toOperatorState )
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
