/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

@FunctionalInterface
public interface TerminationCondition
{
    TerminationCondition CONTINUE_INDEFINITELY = () ->
    {
    };

    /**
     * If store copy client is allowed to continue sending store requests.
     *
     * @throws StoreCopyFailedException if the process should be stopped.
     */
    void assertContinue() throws StoreCopyFailedException;

    /**
     * Chain termination condition with another, returning a {@link CompositeTerminationCondition}.
     *
     * Note: we only have to check whether `other` is a composite condition as the class itself overrides the `and()` method.
     * In other words, `this` knows if it is composite.
     */
    default TerminationCondition and( TerminationCondition other )
    {
        if ( other instanceof CompositeTerminationCondition )
        {
            return other.and( this );
        }
        return new CompositeTerminationCondition( this, other );
    }
}
