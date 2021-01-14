/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.SplittableRandom;

import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;

public class TemporalGenerator
{
    public static final ZoneId[] ZONE_IDS = ZoneId.getAvailableZoneIds().stream().map( ZoneId::of ).toArray( ZoneId[]::new );

    public static ValueGeneratorFactory<ZonedDateTime> dateTime( ValueGeneratorFactory<Long> seconds )
    {
        return new DateTimeGeneratorFactory( seconds );
    }

    public static ValueGeneratorFactory<LocalDateTime> localDatetime( ValueGeneratorFactory<Long> seconds )
    {
        return new LocalDateTimeGeneratorFactory( seconds );
    }

    public static ValueGeneratorFactory<OffsetTime> time( ValueGeneratorFactory<Long> nanosOfDay )
    {
        return new TimeGeneratorFactory( nanosOfDay );
    }

    public static ValueGeneratorFactory<LocalTime> localTime( ValueGeneratorFactory<Long> nanosOfDay )
    {
        return new LocalTimeGeneratorFactory( nanosOfDay );
    }

    public static ValueGeneratorFactory<LocalDate> date( ValueGeneratorFactory<Long> epochDays )
    {
        return new DateGeneratorFactory( epochDays );
    }

    public static ValueGeneratorFactory<DurationValue> duration( ValueGeneratorFactory<Long> nanos )
    {
        return new DurationGeneratorFactory( nanos );
    }

    private static class DateTimeGeneratorFactory implements ValueGeneratorFactory<ZonedDateTime>
    {
        private ValueGeneratorFactory<Long> seconds;

        private DateTimeGeneratorFactory()
        {
        }

        DateTimeGeneratorFactory( ValueGeneratorFactory<Long> seconds )
        {
            this.seconds = seconds;
        }

        @Override
        public ValueGeneratorFun<ZonedDateTime> create()
        {
            ValueGeneratorFun<Long> innerSecondFun = seconds.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerSecondFun.wrapped();
                }

                @Override
                public ZonedDateTime next( SplittableRandom rng )
                {
                    return nextValue( rng ).asObjectCopy();
                }

                @Override
                public DateTimeValue nextValue( SplittableRandom rng )
                {
                    return DateTimeValue.datetime( innerSecondFun.next( rng ), 0, ZoneOffset.UTC );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DateTimeGeneratorFactory that = (DateTimeGeneratorFactory) o;
            return Objects.equals( seconds, that.seconds );
        }

        @Override
        public int hashCode()
        {
            return seconds.hashCode();
        }
    }

    private static class LocalDateTimeGeneratorFactory implements ValueGeneratorFactory<LocalDateTime>
    {
        private ValueGeneratorFactory<Long> seconds;

        private LocalDateTimeGeneratorFactory()
        {
        }

        LocalDateTimeGeneratorFactory( ValueGeneratorFactory<Long> seconds )
        {
            this.seconds = seconds;
        }

        @Override
        public ValueGeneratorFun<LocalDateTime> create()
        {
            ValueGeneratorFun<Long> innerSecondsFun = seconds.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerSecondsFun.wrapped();
                }

                @Override
                public LocalDateTime next( SplittableRandom rng )
                {
                    return LocalDateTime.ofEpochSecond( innerSecondsFun.next( rng ), 0, ZoneOffset.UTC );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return LocalDateTimeValue.localDateTime( next( rng ) );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            LocalDateTimeGeneratorFactory that = (LocalDateTimeGeneratorFactory) o;
            return Objects.equals( seconds, that.seconds );
        }

        @Override
        public int hashCode()
        {
            return seconds.hashCode();
        }
    }

    private static class TimeGeneratorFactory implements ValueGeneratorFactory<OffsetTime>
    {
        private ValueGeneratorFactory<Long> nanos;

        private TimeGeneratorFactory()
        {
        }

        TimeGeneratorFactory( ValueGeneratorFactory<Long> nanos )
        {
            this.nanos = nanos;
        }

        @Override
        public ValueGeneratorFun<OffsetTime> create()
        {
            ValueGeneratorFun<Long> innerNanosOfDayFun = nanos.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerNanosOfDayFun.wrapped();
                }

                @Override
                public OffsetTime next( SplittableRandom rng )
                {
                    return nextValue( rng ).asObjectCopy();
                }

                @Override
                public TimeValue nextValue( SplittableRandom rng )
                {
                    return TimeValue.time( innerNanosOfDayFun.next( rng ), ZoneOffset.UTC );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            TimeGeneratorFactory that = (TimeGeneratorFactory) o;
            return Objects.equals( nanos, that.nanos );
        }

        @Override
        public int hashCode()
        {
            return nanos.hashCode();
        }
    }

    private static class LocalTimeGeneratorFactory implements ValueGeneratorFactory<LocalTime>
    {
        private ValueGeneratorFactory<Long> nanos;

        private LocalTimeGeneratorFactory()
        {
        }

        LocalTimeGeneratorFactory( ValueGeneratorFactory<Long> nanos )
        {
            this.nanos = nanos;
        }

        @Override
        public ValueGeneratorFun<LocalTime> create()
        {
            ValueGeneratorFun<Long> innerNanosOfDayFun = nanos.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerNanosOfDayFun.wrapped();
                }

                @Override
                public LocalTime next( SplittableRandom rng )
                {
                    return LocalTime.ofNanoOfDay( innerNanosOfDayFun.next( rng ) );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return LocalTimeValue.localTime( next( rng ) );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            LocalTimeGeneratorFactory that = (LocalTimeGeneratorFactory) o;
            return Objects.equals( nanos, that.nanos );
        }

        @Override
        public int hashCode()
        {
            return nanos.hashCode();
        }
    }

    private static class DateGeneratorFactory implements ValueGeneratorFactory<LocalDate>
    {
        private ValueGeneratorFactory<Long> epochDays;

        private DateGeneratorFactory()
        {
        }

        DateGeneratorFactory( ValueGeneratorFactory<Long> epochDays )
        {
            this.epochDays = epochDays;
        }

        @Override
        public ValueGeneratorFun<LocalDate> create()
        {
            ValueGeneratorFun<Long> innerEpochDaysFun = epochDays.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerEpochDaysFun.wrapped();
                }

                @Override
                public LocalDate next( SplittableRandom rng )
                {
                    return LocalDate.ofEpochDay( innerEpochDaysFun.next( rng ) );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return DateValue.date( next( rng ) );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DateGeneratorFactory that = (DateGeneratorFactory) o;
            return Objects.equals( epochDays, that.epochDays );
        }

        @Override
        public int hashCode()
        {
            return epochDays.hashCode();
        }
    }

    private static class DurationGeneratorFactory implements ValueGeneratorFactory<DurationValue>
    {
        private ValueGeneratorFactory<Long> nanos;

        private DurationGeneratorFactory()
        {
        }

        DurationGeneratorFactory( ValueGeneratorFactory<Long> nanos )
        {
            this.nanos = nanos;
        }

        @Override
        public ValueGeneratorFun<DurationValue> create()
        {
            ValueGeneratorFun<Long> innerNanosFun = nanos.create();
            return new ValueGeneratorFun<>()
            {
                @Override
                public boolean wrapped()
                {
                    return innerNanosFun.wrapped();
                }

                @Override
                public DurationValue next( SplittableRandom rng )
                {
                    return DurationValue.duration( 0, 0, 0, innerNanosFun.next( rng ) );
                }

                @Override
                public Value nextValue( SplittableRandom rng )
                {
                    return next( rng );
                }
            };
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DurationGeneratorFactory that = (DurationGeneratorFactory) o;
            return Objects.equals( nanos, that.nanos );
        }

        @Override
        public int hashCode()
        {
            return nanos.hashCode();
        }
    }
}
