/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.step;

import com.neo4j.cc_robustness.workload.GraphOperations;
import com.neo4j.cc_robustness.workload.ShutdownType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class RandomStepsGenerator
{
    private final Random random;
    private final int numberOfCcInstances;

    public RandomStepsGenerator( int numberOfCcInstances, long seed )
    {
        this.numberOfCcInstances = numberOfCcInstances;
        this.random = new Random( seed );
    }

    public StepByStepWorkLoad.Step[] generateRandomSteps( int minLength, int maxLength )
    {
        // Which different steps do I have to work with?
        // o Perform operation on a running db
        // o Shut down a running db cleanly (only if all are running)
        // o Shut down a running db harshly (only if all are running)
        // o Start up a non-running db
        // o (after each step) Verify consistency of all running dbs

        int length = minLength + random.nextInt( maxLength - minLength );
        Collection<StepByStepWorkLoad.Step> steps = new ArrayList<>();
        Map<Integer,Boolean> states = new HashMap<>();
        for ( int i = 0; i < numberOfCcInstances; i++ )
        {
            states.put( i, true );
        }
        int upCount = states.size();

        while ( steps.size() < length )
        {
            double r = random.nextDouble();
            if ( r < 0.4 )
            {
                // Perform operation
                GraphOperations.Operation op = GraphOperations.Operation.values()[random.nextInt( GraphOperations.Operation.values().length )];
                steps.add( StepByStepWorkLoad.retriable( new StepByStepWorkLoad.Op( getRandomInstance( states, true ), op ), 2 ) );
            }
            else if ( r < 0.75 )
            {
                // Shut down
                Integer serverId = getRandomInstance( states, true );
                if ( serverId != null && upCount > 1 )
                {
                    steps.add( new StepByStepWorkLoad.Down( serverId, ShutdownType.values()[random.nextInt( ShutdownType.values().length )] ) );
                    states.put( serverId, false );
                    upCount--;
                }
            }
            else
            {
                // Start up
                Integer serverId = getRandomInstance( states, false );
                if ( serverId != null )
                {
                    steps.add( StepByStepWorkLoad.retriable( new StepByStepWorkLoad.Up( serverId ), 2 ) );
                    states.put( serverId, true );
                    upCount++;
                }
            }
        }
        return steps.toArray( new StepByStepWorkLoad.Step[0] );
    }

    private Integer getRandomInstance( Map<Integer,Boolean> states, boolean serverState )
    {
        Set<Integer> tried = new HashSet<>();
        while ( tried.size() < states.size() )
        {
            int id = random.nextInt( states.size() );
            tried.add( id );
            if ( states.get( id ) == serverState )
            {
                return id;
            }
        }
        return null;
    }
}
