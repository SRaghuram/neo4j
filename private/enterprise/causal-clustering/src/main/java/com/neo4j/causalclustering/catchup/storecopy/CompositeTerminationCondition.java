/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.Exceptions;

class CompositeTerminationCondition implements TerminationCondition
{
    private final Set<TerminationCondition> allConditions;

    CompositeTerminationCondition( TerminationCondition first, TerminationCondition... others )
    {
        this.allConditions = Stream.concat( Stream.of( first ), Stream.of( others ) )
                                   .collect( Collectors.toSet() );
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        Exception errors = null;
        for ( TerminationCondition condition : allConditions )
        {
            try
            {
                condition.assertContinue();
            }
            catch ( StoreCopyFailedException e )
            {
                errors = Exceptions.chain( errors, e );
            }
        }

        if ( errors != null )
        {
            throw new StoreCopyFailedException( "One or more termination conditions failed", errors );
        }
    }

    @Override
    public TerminationCondition and( TerminationCondition other )
    {
        if ( other instanceof CompositeTerminationCondition )
        {
            this.allConditions.addAll( ((CompositeTerminationCondition) other).allConditions );
        }
        else
        {
            this.allConditions.add( other );
        }
        return this;
    }
}
