/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.recovery.RecoveryMonitor;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CcStartupTimeoutMonitor implements RecoveryMonitor
{
    private static final int TIME_FACTOR = 3; // for easily scaling all other times

    private static final long DEFAULT_TIMEOUT = millis( 120 * 10 ); // timeouts not specifically designed for

    private static final long RECOVERY_ITERATION_TIMEOUT = millis( 60 );

    private long startTimeMillis;

    private long recoveryStartMillis;
    private long lastRecoveredTransactionMillis;
    private long recoveryCompleteMillis;

    private String state = "PRE STARTUP";

    private static long millis( int seconds )
    {
        return SECONDS.toMillis( seconds * TIME_FACTOR );
    }

    public void start()
    {
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public void recoveryRequired( LogPosition recoveryPosition )
    {
        /* Called at start of recovery iff required. */
        state = "RECOVERY STARTED";
        lastRecoveredTransactionMillis = recoveryStartMillis = System.currentTimeMillis();
    }

    @Override
    public void recoveryCompleted( int numberOfRecoveredTransactions, long recoveryTimeInMilliseconds )
    {
        state = "DONE WITH RECOVERY";
        recoveryCompleteMillis = System.currentTimeMillis();
    }

    long getApplicableEndTimeMillis()
    {
        // The conditional statements are in the backwards order
        // of expected occurrence, giving an easy way of searching for
        // and mapping the current state to a desired timeout. We do
        // not verify that the state progression is as expected.
        //
        // Each defined timeout is thus the time allowed for that
        // particular state. We are in a particular state when
        // the start (start/end) of it has been observed and a
        // time for it has been saved in the respective variable.

        if ( recoveryCompleteMillis > 0 )
        {
            return recoveryCompleteMillis + DEFAULT_TIMEOUT;
        }
        else if ( recoveryStartMillis > 0 )
        {
            return lastRecoveredTransactionMillis + RECOVERY_ITERATION_TIMEOUT;
        }
        else
        {
            return startTimeMillis + DEFAULT_TIMEOUT;
        }
    }

    @Override
    public String toString()
    {
        return state;
    }
}
