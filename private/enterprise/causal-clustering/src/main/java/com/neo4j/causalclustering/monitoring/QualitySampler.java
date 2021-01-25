/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.util.Preconditions.requirePositive;

/**
 * A helper class for sampling a value and associating a trustworthy timestamp with it.
 * <p>
 * Samples which are not within the time tolerance will be discarded and re-sampling
 * attempted up to the maximum number configured. The typical thing that this sampler
 * tries to protect against is skew between the sampled value and the timestamp, due
 * to for example untimely preemption or garbage collections.
 */
class QualitySampler<T>
{
    private static final int DEFAULT_MAXIMUM_ATTEMPTS = 16;

    private final Clock clock;
    private final Duration samplingTolerance;
    private final int maximumAttempts;
    private final Supplier<T> sampler;

    QualitySampler( Clock clock, Duration samplingTolerance, Supplier<T> sampler )
    {
        this( clock, samplingTolerance, DEFAULT_MAXIMUM_ATTEMPTS, sampler );
    }

    QualitySampler( Clock clock, Duration samplingTolerance, int maximumAttempts, Supplier<T> sampler )
    {
        requirePositive( maximumAttempts );

        this.clock = clock;
        this.samplingTolerance = samplingTolerance;
        this.maximumAttempts = maximumAttempts;
        this.sampler = sampler;
    }

    public Optional<Sample<T>> sample()
    {
        int attempt = 0;
        Sample<T> sample = null;
        while ( sample == null && attempt < maximumAttempts )
        {
            sample = sample0();
            attempt++;
        }
        return Optional.ofNullable( sample );
    }

    private Sample<T> sample0()
    {
        Instant before = clock.instant();
        T sample = sampler.get();
        Instant after = clock.instant();

        Duration samplingDuration = Duration.between( before, after );

        if ( samplingDuration.compareTo( samplingTolerance ) > 0 )
        {
            return null;
        }

        Instant sampleTime = before.plus( samplingDuration.dividedBy( 2 ) );
        return new Sample<>( sampleTime, sample );
    }
}
