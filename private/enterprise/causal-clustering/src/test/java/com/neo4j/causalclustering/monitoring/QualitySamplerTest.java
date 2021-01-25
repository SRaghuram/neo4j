/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.time.FakeClock;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QualitySamplerTest
{
    private FakeClock clock = new FakeClock();

    private Duration tolerance = Duration.of( 1, SECONDS );
    private Duration moreThanTolerance = tolerance.plus( 1, MILLIS );

    @Test
    void shouldSampleCorrectTimeAndValue()
    {
        // given
        Object expected = new Object();
        Supplier<Object> sampler = () -> expected;
        clock.forward( Duration.of( 5, SECONDS ) );

        QualitySampler<Object> qualitySampler = new QualitySampler<>( clock, tolerance, 1, sampler );

        // when
        Optional<Sample<Object>> optSample = qualitySampler.sample();

        // then
        assertTrue( optSample.isPresent() );

        Sample<Object> sample = optSample.get();
        assertEquals( expected, sample.value() );
        assertEquals( clock.instant(), sample.instant() );
    }

    @Test
    void shouldAcceptSampleWithinToleranceWindow()
    {
        // given
        Supplier<Object> sampler = () ->
        {
            clock.forward( tolerance );
            return new Object();
        };

        QualitySampler<Object> qualitySampler = new QualitySampler<>( clock, tolerance, 1, sampler );

        // when
        Optional<Sample<Object>> optSample = qualitySampler.sample();

        // then
        assertTrue( optSample.isPresent() );
    }

    @Test
    void shouldNotAcceptSamplesOutsideToleranceWindow()
    {
        // given
        Supplier<Object> sampler = () ->
        {
            clock.forward( moreThanTolerance );
            return new Object();
        };

        QualitySampler<Object> qualitySampler = new QualitySampler<>( clock, tolerance, 3, sampler );

        // when
        Optional<Sample<Object>> optSample = qualitySampler.sample();

        // then
        assertFalse( optSample.isPresent() );
    }

    @Test
    void shouldRetrySamplingUntilMaximumConfiguredNumberOfAttempts()
    {
        // given
        MutableInt attempts = new MutableInt();
        Supplier<Object> sampler = () ->
        {
            attempts.increment();
            clock.forward( moreThanTolerance );
            return new Object();
        };

        int maximumAttempts = 4;
        QualitySampler<Object> qualitySampler = new QualitySampler<>( clock, tolerance, maximumAttempts, sampler );

        // when
        Optional<Sample<Object>> optSample = qualitySampler.sample();

        // then
        assertFalse( optSample.isPresent() );
        assertEquals( maximumAttempts, attempts.intValue() );
    }

    @Test
    void shouldReturnSampleInstantAtMidpoint()
    {
        Duration delta = Duration.of( 10, MILLIS );
        Supplier<Object> sampler = () ->
        {
            clock.forward( delta.multipliedBy( 2 ) );
            return new Object();
        };

        QualitySampler<Object> qualitySampler = new QualitySampler<>( clock, tolerance, 1, sampler );

        // when
        Optional<Sample<Object>> optSample = qualitySampler.sample();

        // then
        assertTrue( optSample.isPresent() );
        assertEquals( clock.instant().minus( delta ), optSample.get().instant() );
    }
}
