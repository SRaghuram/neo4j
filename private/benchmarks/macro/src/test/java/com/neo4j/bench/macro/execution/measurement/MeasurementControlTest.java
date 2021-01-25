/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.measurement;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MeasurementControlTest
{
    @Test
    public void shouldCompleteImmediatelyForNonePolicy()
    {
        // given
        MeasurementControl m = MeasurementControl.none();

        assertThat( m.isComplete(), equalTo( true ) );

        // when
        m.reset();

        // then
        assertThat( m.isComplete(), equalTo( true ) );
        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );

        // when
        m.register( 0 );
        // measurement control should be true no matter how many times a result is registered
        assertThat( m.isComplete(), equalTo( true ) );
    }

    @Test
    public void shouldCompleteAfterSingle()
    {
        // given
        MeasurementControl m = MeasurementControl.single();

        assertThat( m.isComplete(), equalTo( false ) );

        // when
        m.register( 0 );

        // then
        assertThat( m.isComplete(), equalTo( true ) );
        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );
    }

    @Test
    public void shouldCompleteAfterCount()
    {
        //given
        int maxCount = 50;

        // when
        MeasurementControl m = createAndAdvanceCount( maxCount, maxCount - 1 );

        // then
        m.register( 0 );
        assertThat( m.isComplete(), equalTo( true ) );
        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );
        m.reset();
        assertThat( m.isComplete(), equalTo( false ) );
    }

    @Test
    public void shouldCompleteAfterDuration()
    {
        // given
        int maxSeconds = 10;
        TestClock clock = new TestClock( 0 );

        // when
        MeasurementControl m = createAndAdvanceDuration( maxSeconds, maxSeconds - 1, clock );

        // then
        clock.set( Duration.ofSeconds( maxSeconds ).toMillis() );
        assertThat( m.isComplete(), equalTo( true ) );
        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );
        m.reset();
        assertThat( m.isComplete(), equalTo( false ) );
    }

    @Test
    public void shouldCompleteWhenAnyComplete()
    {
        //given
        int maxCount = 50;

        // when
        MeasurementControl m1 = createAndAdvanceCount( maxCount, maxCount - 1 );
        MeasurementControl m2 = createAndAdvanceCount( maxCount, maxCount - 1 );
        MeasurementControl m3 = createAndAdvanceCount( maxCount, maxCount - 1 );

        MeasurementControl m = MeasurementControl.or( m1, m2, m3 );
        assertThat( m1.isComplete(), equalTo( false ) );
        assertThat( m2.isComplete(), equalTo( false ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );

        // then
        m2.register( 0 );
        assertThat( m1.isComplete(), equalTo( false ) );
        assertThat( m2.isComplete(), equalTo( true ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( true ) );
        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );

        m.reset();
        assertThat( m1.isComplete(), equalTo( false ) );
        assertThat( m2.isComplete(), equalTo( false ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );
    }

    @Test
    public void shouldCompleteOnlyAfterAllComplete()
    {
        //given
        int maxCount = 50;

        // when
        MeasurementControl m1 = createAndAdvanceCount( maxCount, maxCount - 1 );
        MeasurementControl m2 = createAndAdvanceCount( maxCount, maxCount - 1 );
        MeasurementControl m3 = createAndAdvanceCount( maxCount, maxCount - 1 );

        MeasurementControl m = MeasurementControl.and( m1, m2, m3 );
        assertThat( m1.isComplete(), equalTo( false ) );
        assertThat( m2.isComplete(), equalTo( false ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );

        // then
        m1.register( 0 );
        assertThat( m1.isComplete(), equalTo( true ) );
        assertThat( m2.isComplete(), equalTo( false ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );

        m2.register( 0 );
        assertThat( m1.isComplete(), equalTo( true ) );
        assertThat( m2.isComplete(), equalTo( true ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );

        m3.register( 0 );
        assertThat( m1.isComplete(), equalTo( true ) );
        assertThat( m2.isComplete(), equalTo( true ) );
        assertThat( m3.isComplete(), equalTo( true ) );
        assertThat( m.isComplete(), equalTo( true ) );

        // after being true once, measurement control should be true no matter how many times it is checked, until it is reset again
        assertThat( m.isComplete(), equalTo( true ) );

        m.reset();
        assertThat( m1.isComplete(), equalTo( false ) );
        assertThat( m2.isComplete(), equalTo( false ) );
        assertThat( m3.isComplete(), equalTo( false ) );
        assertThat( m.isComplete(), equalTo( false ) );
    }

    private MeasurementControl createAndAdvanceCount( int max, int advanceTo )
    {
        MeasurementControl m = MeasurementControl.ofCount( max );
        for ( int i = 0; i < advanceTo; i++ )
        {
            assertThat( m.isComplete(), equalTo( false ) );
            m.register( 0 );
        }
        return m;
    }

    private MeasurementControl createAndAdvanceDuration( int maxSeconds, int advanceTo, TestClock clock )
    {
        MeasurementControl m = new DurationMeasurementControl( clock, Duration.ofSeconds( maxSeconds ) );
        for ( int i = 0; i < advanceTo; i++ )
        {
            clock.set( Duration.ofSeconds( i ).toMillis() );
            assertThat( m.isComplete(), equalTo( false ) );
        }
        return m;
    }
}
