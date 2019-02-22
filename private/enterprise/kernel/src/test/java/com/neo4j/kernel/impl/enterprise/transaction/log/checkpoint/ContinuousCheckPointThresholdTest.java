/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContinuousCheckPointThresholdTest
{
    @Test
    void continuousCheckPointMustReachThresholdOnEveryCommit()
    {
        ContinuousCheckPointThreshold threshold = new ContinuousCheckPointThreshold();
        threshold.initialize( 10 );
        assertFalse( threshold.thresholdReached( 10 ) );
        assertTrue( threshold.thresholdReached( 11 ) );
        assertTrue( threshold.thresholdReached( 11 ) );
        threshold.checkPointHappened( 12 );
        assertFalse( threshold.thresholdReached( 12 ) );
    }

    @Test
    void continuousThresholdMustNotBusySpin()
    {
        ContinuousCheckPointThreshold threshold = new ContinuousCheckPointThreshold();
        assertThat( threshold.checkFrequencyMillis(), greaterThan( 0L ) );
    }
}
