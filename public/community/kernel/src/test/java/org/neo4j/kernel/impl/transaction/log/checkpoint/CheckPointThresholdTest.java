/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold.DEFAULT_CHECKING_FREQUENCY_MILLIS;

class CheckPointThresholdTest extends CheckPointThresholdTestSupport
{
    @Test
    void mustCreateThresholdThatTriggersAfterTransactionCount()
    {
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 1 ); // Initialise at transaction id offset by 1.

        // False because we're not yet at threshold.
        assertFalse( threshold.isCheckPointingNeeded( intervalTx - 1, notTriggered ) );
        // Still false because the counter is offset by one, since we initialised with 1.
        assertFalse( threshold.isCheckPointingNeeded( intervalTx, notTriggered ) );
        // True because new we're at intervalTx + initial offset.
        assertTrue( threshold.isCheckPointingNeeded( intervalTx + 1, triggered ) );
        verifyTriggered( "every 100000 transactions" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustCreateThresholdThatTriggersAfterTime()
    {
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 1 );
        // Skip the initial wait period.
        clock.forward( intervalTime.toMillis(), MILLISECONDS );
        // The clock will trigger at a random point within the interval in the future.

        // False because we haven't moved the clock, or the transaction count.
        assertFalse( threshold.isCheckPointingNeeded( 2, notTriggered ) );
        // True because we now moved forward by an interval.
        clock.forward( intervalTime.toMillis(), MILLISECONDS );
        assertTrue( threshold.isCheckPointingNeeded( 4, triggered ) );
        verifyTriggered( "every 15 minutes threshold" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustNotTriggerBeforeTimeWithTooFewCommittedTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 50, MILLISECONDS );
        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
    }

    @Test
    void mustTriggerWhenTimeThresholdIsReachedAndThereAreCommittedTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );

        assertTrue( threshold.isCheckPointingNeeded( 42, triggered ) );
        verifyTriggered( "every 100 milliseconds" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustTriggerWhenWeirdTimeThresholdIsReachedAndThereAreCommittedTransactions()
    {
        withIntervalTime( "1100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 2199, MILLISECONDS );

        assertTrue( threshold.isCheckPointingNeeded( 42, triggered ) );
        verifyTriggered( "every 1 seconds 100 milliseconds" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustNotTriggerWhenTimeThresholdIsReachedAndThereAreNoCommittedTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 42 );

        clock.forward( 199, MILLISECONDS );

        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @Test
    void mustNotTriggerPastTimeThresholdSinceLastCheckpointWithNoNewTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );
        threshold.checkPointHappened( 42 );
        clock.forward( 100, MILLISECONDS );

        assertFalse( threshold.isCheckPointingNeeded( 42, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @Test
    void mustTriggerPastTimeThresholdSinceLastCheckpointWithNewTransactions()
    {
        withIntervalTime( "100ms" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        clock.forward( 199, MILLISECONDS );
        threshold.checkPointHappened( 42 );
        clock.forward( 100, MILLISECONDS );

        assertTrue( threshold.isCheckPointingNeeded( 43, triggered ) );
        verifyTriggered( "every 100 milliseconds" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustNotTriggerOnTransactionCountWhenThereAreNoNewTransactions()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertFalse( threshold.isCheckPointingNeeded( 2, notTriggered ) );
    }

    @Test
    void mustNotTriggerOnTransactionCountWhenCountIsBellowThreshold()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertFalse( threshold.isCheckPointingNeeded( 3, notTriggered ) );
    }

    @Test
    void mustTriggerOnTransactionCountWhenCountIsAtThreshold()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertTrue( threshold.isCheckPointingNeeded( 4, triggered ) );
        verifyTriggered( "every 2 transactions" );
        verifyNoMoreTriggers();
    }

    @Test
    void mustNotTriggerOnTransactionCountAtThresholdIfCheckPointAlreadyHappened()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertFalse( threshold.isCheckPointingNeeded( 4, notTriggered ) );
    }

    @Test
    void mustNotTriggerWhenTransactionCountIsWithinThresholdSinceLastTrigger()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertFalse( threshold.isCheckPointingNeeded( 5, notTriggered ) );
    }

    @Test
    void mustTriggerOnTransactionCountWhenCountIsAtThresholdSinceLastCheckPoint()
    {
        withIntervalTx( 2 );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        threshold.checkPointHappened( 4 );
        assertTrue( threshold.isCheckPointingNeeded( 6, triggered ) );
        verifyTriggered( "2 transactions" );
        verifyNoMoreTriggers();
    }

    @Test
    void timeBasedThresholdMustSuggestSchedulingFrequency()
    {
        // By default, the transaction count based threshold wants a higher check frequency than the time based
        // default threshold.
        assertThat( createThreshold().checkFrequencyMillis() ).isEqualTo( DEFAULT_CHECKING_FREQUENCY_MILLIS );

        withIntervalTime( "100ms" );
        assertThat( createThreshold().checkFrequencyMillis() ).isEqualTo( 100L );
    }
}
