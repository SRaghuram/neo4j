/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import com.neo4j.causalclustering.helper.SuspendableLifecycleStateTestHelpers.LifeCycleState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class SuspendableLifeCycleLifeStateChangeTest
{
    @Parameterized.Parameter()
    public LifeCycleState fromState;

    @Parameterized.Parameter( 1 )
    public SuspendableLifecycleStateTestHelpers.SuspendedState fromSuspendedState;

    @Parameterized.Parameter( 2 )
    public LifeCycleState toLifeCycleState;

    @Parameterized.Parameter( 3 )
    public LifeCycleState shouldBeRunning;

    @Parameterized.Parameters( name = "From {0} and {1} to {2} should end in {3}" )
    public static Iterable<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();
        for ( LifeCycleState lifeCycleState : LifeCycleState.values() )
        {
            for ( SuspendableLifecycleStateTestHelpers.SuspendedState suspendedState : SuspendableLifecycleStateTestHelpers.SuspendedState.values() )
            {
                for ( LifeCycleState toState : lifeCycleOperation() )
                {
                    params.add( new Object[]{lifeCycleState, suspendedState, toState, expectedResult( suspendedState, toState )} );
                }
            }
        }
        return params;
    }

    private StateAwareSuspendableLifeCycle lifeCycle;

    private static LifeCycleState[] lifeCycleOperation()
    {
        return new LifeCycleState[]{LifeCycleState.Start, LifeCycleState.Stop};
    }

    @Before
    public void setUpServer() throws Throwable
    {
        lifeCycle = new StateAwareSuspendableLifeCycle( new AssertableLogProvider( false ).getLog( "log" ) );
        SuspendableLifecycleStateTestHelpers.setInitialState( lifeCycle, fromState );
        fromSuspendedState.set( lifeCycle );
    }

    @Test
    public void changeLifeState() throws Throwable
    {
        toLifeCycleState.set( lifeCycle );
        assertEquals( shouldBeRunning, lifeCycle.status );
    }

    private static LifeCycleState expectedResult( SuspendableLifecycleStateTestHelpers.SuspendedState state, LifeCycleState toLifeCycle )
    {
        if ( state == SuspendableLifecycleStateTestHelpers.SuspendedState.Untouched || state == SuspendableLifecycleStateTestHelpers.SuspendedState.Enabled )
        {
            return toLifeCycle;
        }
        else if ( state == SuspendableLifecycleStateTestHelpers.SuspendedState.Disabled )
        {
            if ( toLifeCycle == LifeCycleState.Shutdown )
            {
                return toLifeCycle;
            }
            else
            {
                return LifeCycleState.Stop;
            }
        }
        else
        {
            throw new IllegalStateException( "Unknown state " + state );
        }
    }
}
