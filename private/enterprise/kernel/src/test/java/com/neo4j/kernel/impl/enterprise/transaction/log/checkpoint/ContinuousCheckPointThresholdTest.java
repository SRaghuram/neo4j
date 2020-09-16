/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContinuousCheckPointThresholdTest
{
    @Test
    void continuousCheckPointMustReachThresholdOnEveryCommit()
    {
        ContinuousCheckPointThreshold threshold = new ContinuousCheckPointThreshold();
        threshold.initialize( 10 );
        assertFalse( threshold.thresholdReached( 10, 99 ) );
        assertTrue( threshold.thresholdReached( 11, 99 ) );
        assertTrue( threshold.thresholdReached( 11, 99 ) );
        threshold.checkPointHappened( 12 );
        assertFalse( threshold.thresholdReached( 12, 99 ) );
    }

    @Test
    void continuousThresholdMustNotBusySpin()
    {
        ContinuousCheckPointThreshold threshold = new ContinuousCheckPointThreshold();
        assertThat( threshold.checkFrequencyMillis() ).isGreaterThan( 0L );
    }
}
