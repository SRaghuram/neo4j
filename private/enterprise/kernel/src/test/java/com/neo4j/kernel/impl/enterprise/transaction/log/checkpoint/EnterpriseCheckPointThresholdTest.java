/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdTestSupport;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EnterpriseCheckPointThresholdTest extends CheckPointThresholdTestSupport
{
    private boolean haveLogsToPrune;

    @BeforeEach
    @Override
    public void setUp()
    {
        super.setUp();
        logPruning = new LogPruning()
        {
            @Override
            public void pruneLogs( long currentVersion )
            {
                Assertions.fail( "Check point threshold must never call out to prune logs directly." );
            }

            @Override
            public boolean mightHaveLogsToPrune( long upperVersion )
            {
                return haveLogsToPrune;
            }

            @Override
            public String describeCurrentStrategy()
            {
                return "test pruning strategy";
            }
        };
    }

    @Test
    void checkPointIsNeededIfWeMightHaveLogsToPrune()
    {
        withPolicy( "volumetric" );
        haveLogsToPrune = true;
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );
        assertTrue( threshold.isCheckPointingNeeded( 2, ARBITRARY_LOG_VERSION, triggered ) );
        verifyTriggered( "log pruning", "test pruning strategy" );
        verifyNoMoreTriggers();
    }

    @Test
    void checkPointIsInitiallyNotNeededIfWeHaveNoLogsToPrune()
    {
        withPolicy( "volumetric" );
        haveLogsToPrune = false;
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );
        assertFalse( threshold.isCheckPointingNeeded( 2, ARBITRARY_LOG_VERSION, notTriggered ) );
        verifyNoMoreTriggers();
    }

    @Test
    void continuousPolicyMustTriggerCheckPointsAfterAnyWriteTransaction()
    {
        withPolicy( "continuous" );
        CheckPointThreshold threshold = createThreshold();
        threshold.initialize( 2 );

        assertThat( threshold.checkFrequencyMillis() ).isLessThan( CheckPointThreshold.DEFAULT_CHECKING_FREQUENCY_MILLIS );

        assertFalse( threshold.isCheckPointingNeeded( 2, ARBITRARY_LOG_VERSION, triggered ) );
        threshold.checkPointHappened( 3 );
        assertFalse( threshold.isCheckPointingNeeded( 3, ARBITRARY_LOG_VERSION, triggered ) );
        assertTrue( threshold.isCheckPointingNeeded( 4, ARBITRARY_LOG_VERSION, triggered ) );
        verifyTriggered( "continuous" );
        verifyNoMoreTriggers();
    }
}
