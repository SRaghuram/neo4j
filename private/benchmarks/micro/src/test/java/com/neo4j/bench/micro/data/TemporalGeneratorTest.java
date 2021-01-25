/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.SplittableRandom;
import java.util.function.BiFunction;

import org.neo4j.values.storable.DurationValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TemporalGeneratorTest
{
    @Test
    public void shouldGenerateDateTimes()
    {
        ZonedDateTime initial = ZonedDateTime.ofInstant( Instant.ofEpochSecond( 1, 0 ), ZoneOffset.UTC );
        ValueGeneratorFactory<Long> seconds = NumberGenerator.ascLong( initial.toEpochSecond() );
        ValueGeneratorFactory<ZonedDateTime> dateTimes = TemporalGenerator.dateTime( seconds );
        assertCorrect( dateTimes, initial, ZonedDateTime::plusSeconds );
    }

    @Test
    public void shouldGenerateLocalDateTimes()
    {
        LocalDateTime initial = LocalDateTime.ofEpochSecond( 1, 0, ZoneOffset.UTC );
        ValueGeneratorFactory<Long> seconds = NumberGenerator.ascLong( initial.toEpochSecond( ZoneOffset.UTC ) );
        ValueGeneratorFactory<LocalDateTime> localDateTimes = TemporalGenerator.localDatetime( seconds );
        assertCorrect( localDateTimes, initial, LocalDateTime::plusSeconds );
    }

    @Test
    public void shouldGenerateTimes()
    {
        OffsetTime initial = OffsetTime.ofInstant( Instant.ofEpochSecond( 0, 1 ), ZoneOffset.UTC );
        ValueGeneratorFactory<Long> nanos = NumberGenerator.ascLong( initial.getNano() );
        ValueGeneratorFactory<OffsetTime> times = TemporalGenerator.time( nanos );
        assertCorrect( times, initial, OffsetTime::plusNanos );
    }

    @Test
    public void shouldGenerateLocalTimes()
    {
        LocalTime initial = LocalTime.ofNanoOfDay( 1 );
        ValueGeneratorFactory<Long> nanos = NumberGenerator.ascLong( initial.toNanoOfDay() );
        ValueGeneratorFactory<LocalTime> localTimes = TemporalGenerator.localTime( nanos );
        assertCorrect( localTimes, initial, LocalTime::plusNanos );
    }

    @Test
    public void shouldGenerateDates()
    {
        ZonedDateTime initial = ZonedDateTime.ofInstant( Instant.ofEpochSecond( 1, 0 ), ZoneOffset.UTC );
        ValueGeneratorFactory<Long> seconds = NumberGenerator.ascLong( initial.toEpochSecond() );
        ValueGeneratorFactory<ZonedDateTime> zonedDateTimes = TemporalGenerator.dateTime( seconds );
        assertCorrect( zonedDateTimes, initial, ZonedDateTime::plusSeconds );
    }

    @Test
    public void shouldGenerateDurations()
    {
        DurationValue initial = DurationValue.duration( 0, 0, 0, 1 );
        ValueGeneratorFactory<Long> nanos = NumberGenerator.ascLong( initial.get( ChronoUnit.NANOS ) );
        ValueGeneratorFactory<DurationValue> durations = TemporalGenerator.duration( nanos );
        assertCorrect( durations, initial, ( duration, value ) -> duration.plus( value, ChronoUnit.NANOS ) );
    }

    private <T> void assertCorrect( ValueGeneratorFactory<T> factory, T initial, BiFunction<T,Integer,T> incrementFun )
    {
        SplittableRandom rng = new SplittableRandom( 42L );
        for ( int i = 0; i < 10; i++ )
        {
            ValueGeneratorFun<T> fun = factory.create();
            T first = fun.next( rng );
            assertThat( first, equalTo( initial ) );

            for ( int j = 1; j < 11; j++ )
            {
                assertThat(
                        fun.next( rng ),
                        equalTo( incrementFun.apply( first, j ) ) );
            }
        }
    }
}
