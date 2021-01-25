/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import java.text.DecimalFormat;
import java.util.Calendar;

import static java.lang.String.format;

public abstract class LdbcDateCodec
{
    public enum Format
    {
        STRING_ENCODED,
        NUMBER_UTC,
        NUMBER_ENCODED
    }

    public enum Resolution
    {
        NOT_APPLICABLE,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND
    }

    public static LdbcDateCodec codecFor( Resolution resolution )
    {
        switch ( resolution )
        {
        case YEAR:
            return new YearCodec();
        case MONTH:
            return new MonthCodec();
        case DAY:
            return new DayCodec();
        case HOUR:
            return new HourCodec();
        case MINUTE:
            return new MinuteCodec();
        case SECOND:
            return new SecondCodec();
        case MILLISECOND:
            return new MilliSecondCodec();
        case NOT_APPLICABLE:
            return new NotApplicableCodec();
        default:
            throw new RuntimeException( format( "Unsupported resolution: %s", resolution ) );
        }
    }

    /**
     * Resolution to truncate dates to, e.g., yyyymmdd
     *
     * @return resolution
     */
    public abstract Resolution resolution();

    /**
     * Convert UTC encoded date time to encoded number (i.e. yyyymmddhhmmssmmm) format, at configured resolution
     *
     * @return date encoded at configured resolution
     */
    public abstract long utcToEncodedDateAtResolution( long utc, Calendar calendar );

    /**
     * Convert encoded number (i.e. yyyymmddhhmmssmmm) date time format, encoded number at configured resolution
     *
     * @return date encoded at configured resolution
     */
    public abstract long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime );

    /**
     * Convert encoded number at configured resolution (e.g. yyyymm) to string
     *
     * @return string representation of date encoded at configured resolution
     */
    public abstract String encodedDateAtResolutionToString( long encodedDateTimeAtResolution );

    /**
     * Return array representing range of encoded number date times, each at configured resolution
     *
     * @return array of dates, between bounds, encoded at configured resolution, ordered ascending
     */
    public long[] encodedDatesAtResolutionForRange(
            long lowerAtResolution,
            long upperAtResolution,
            Calendar calendar )
    {
        int requiredArraySize = 1;
        populateCalendarFromEncodedDateAtResolution( lowerAtResolution, calendar );
        while ( calendarToEncodedDateAtResolution( calendar ) < upperAtResolution )
        {
            incrementCalendarByTimestampResolution( calendar, 1 );
            requiredArraySize++;
        }
        long[] encodedDatesAtResolution = new long[requiredArraySize];
        populateCalendarFromEncodedDateAtResolution( lowerAtResolution, calendar );
        for ( int i = 0; i < requiredArraySize; i++ )
        {
            encodedDatesAtResolution[i] = calendarToEncodedDateAtResolution( calendar );
            incrementCalendarByTimestampResolution( calendar, 1 );
        }
        return encodedDatesAtResolution;
    }

    /**
     * Populate calendar fields according to value of date, encoded at configured resolution (e.g. yyyymm)
     *
     * @param encodedDateAtResolution date encoded at configured resolution
     * @param calendar mutable calendar instance
     */
    public abstract void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar );

    /**
     * Increment appropriate calendar field, according to configured resolution
     *
     * @param calendar mutable calendar instance
     * @param increment amount (units dependent on configured resolution) to increment calendar by
     */
    public abstract void incrementCalendarByTimestampResolution( Calendar calendar, int increment );

    /**
     * Convert contents of calendar to encoded number date, at configured resolution
     *
     * @param calendar calendar to read date from
     * @return encoded date at configured resolution
     */
    public abstract long calendarToEncodedDateAtResolution( Calendar calendar );

    private static class YearCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "0000" );
        private final long shift = 10_000_000_000_000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.YEAR;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYY
            int year = (int) encodedDateAtResolution;
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = 0;
            int day = 1;
            int hour = 0;
            int minute = 0;
            int second = 0;
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.YEAR, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            // YYYY
            return calendar.get( Calendar.YEAR );
        }
    }

    private static class MonthCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "000000" );
        private final long shift = 100_000_000_000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.MONTH;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMM
            int year = (int) (encodedDateAtResolution / 100);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) (encodedDateAtResolution % 100) - 1;
            int day = 1;
            int hour = 0;
            int minute = 0;
            int second = 0;
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.MONTH, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            return (year * 100L) +
                   month;
        }
    }

    private static class DayCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "00000000" );
        private final long shift = 1000_000_000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.DAY;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMMDD
            int year = (int) (encodedDateAtResolution / 10_000);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) ((encodedDateAtResolution / 100) % 100) - 1;
            int day = (int) (encodedDateAtResolution % 100);
            int hour = 0;
            int minute = 0;
            int second = 0;
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.DAY_OF_MONTH, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            int day = calendar.get( Calendar.DAY_OF_MONTH );
            return (year * 10_000L) +
                   (month * 100L) +
                   day;
        }
    }

    private static class HourCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "0000000000" );
        private final long shift = 10_000_000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.HOUR;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMMDDhh
            int year = (int) (encodedDateAtResolution / 1000_000);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) ((encodedDateAtResolution / 10_000) % 100) - 1;
            int day = (int) ((encodedDateAtResolution / 100) % 100);
            int hour = (int) (encodedDateAtResolution % 100);
            int minute = 0;
            int second = 0;
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.HOUR_OF_DAY, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            int day = calendar.get( Calendar.DAY_OF_MONTH );
            int hour = calendar.get( Calendar.HOUR_OF_DAY );
            return (year * 1000_000L) +
                   (month * 10_000L) +
                   (day * 100L) +
                   hour;
        }
    }

    private static class MinuteCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "000000000000" );
        private final long shift = 100_000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.MINUTE;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMMDDhhmm
            int year = (int) (encodedDateAtResolution / 100_000_000);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) ((encodedDateAtResolution / 1000_000) % 100) - 1;
            int day = (int) ((encodedDateAtResolution / 10_000) % 100);
            int hour = (int) ((encodedDateAtResolution / 100) % 100);
            int minute = (int) (encodedDateAtResolution % 100);
            int second = 0;
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.MINUTE, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            int day = calendar.get( Calendar.DAY_OF_MONTH );
            int hour = calendar.get( Calendar.HOUR_OF_DAY );
            int minute = calendar.get( Calendar.MINUTE );
            return (year * 100_000_000L) +
                   (month * 1000_000L) +
                   (day * 10_000L) +
                   (hour * 100L) +
                   minute;
        }
    }

    private static class SecondCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "00000000000000" );
        private final long shift = 1000L;

        @Override
        public Resolution resolution()
        {
            return Resolution.SECOND;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime / shift;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMMDDhhmmss
            int year = (int) (encodedDateAtResolution / 10_000_000_000L);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) ((encodedDateAtResolution / 100_000_000) % 100) - 1;
            int day = (int) ((encodedDateAtResolution / 1000_000) % 100);
            int hour = (int) ((encodedDateAtResolution / 10_000) % 100);
            int minute = (int) ((encodedDateAtResolution / 100) % 100);
            int second = (int) (encodedDateAtResolution % 100);
            int millisecond = 0;
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.SECOND, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            int day = calendar.get( Calendar.DAY_OF_MONTH );
            int hour = calendar.get( Calendar.HOUR_OF_DAY );
            int minute = calendar.get( Calendar.MINUTE );
            int second = calendar.get( Calendar.SECOND );
            return (year * 10_000_000_000L) +
                   (month * 100_000_000L) +
                   (day * 1000_000L) +
                   (hour * 10_000L) +
                   (minute * 100L) +
                   second;
        }
    }

    private static class MilliSecondCodec extends LdbcDateCodec
    {
        private static final DecimalFormat STRING_FORMAT = new DecimalFormat( "00000000000000000" );

        @Override
        public Resolution resolution()
        {
            return Resolution.MILLISECOND;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            calendar.setTimeInMillis( utc );
            return calendarToEncodedDateAtResolution( calendar );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            // YYYYMMDDhhmmssMMM
            return encodedDateTime;
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            return STRING_FORMAT.format( encodedDateTimeAtResolution );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            // YYYYMMDDhhmmssMMM
            int year = (int) (encodedDateAtResolution / 10_000_000_000_000L);
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = (int) ((encodedDateAtResolution / 100_000_000_000L) % 100) - 1;
            int day = (int) ((encodedDateAtResolution / 1000_000_000) % 100);
            int hour = (int) ((encodedDateAtResolution / 10_000_000) % 100);
            int minute = (int) ((encodedDateAtResolution / 100_000) % 100);
            int second = (int) ((encodedDateAtResolution / 1000) % 100);
            int millisecond = (int) (encodedDateAtResolution % 1000);
            calendar.set( Calendar.YEAR, year );
            calendar.set( Calendar.MONTH, month );
            calendar.set( Calendar.DAY_OF_MONTH, day );
            calendar.set( Calendar.HOUR_OF_DAY, hour );
            calendar.set( Calendar.MINUTE, minute );
            calendar.set( Calendar.SECOND, second );
            calendar.set( Calendar.MILLISECOND, millisecond );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            calendar.add( Calendar.MILLISECOND, increment );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            int year = calendar.get( Calendar.YEAR );
            // Calendar.get(Calendar.MONTH) returns 0-11, add 1 so months are in range 1-12
            int month = calendar.get( Calendar.MONTH ) + 1;
            int day = calendar.get( Calendar.DAY_OF_MONTH );
            int hour = calendar.get( Calendar.HOUR_OF_DAY );
            int minute = calendar.get( Calendar.MINUTE );
            int second = calendar.get( Calendar.SECOND );
            int millisecond = calendar.get( Calendar.MILLISECOND );
            return (year * 10_000_000_000_000L) +
                   (month * 100_000_000_000L) +
                   (day * 1000_000_000L) +
                   (hour * 10_000_000L) +
                   (minute * 100_000L) +
                   (second * 1000L) +
                   millisecond;
        }
    }

    private static class NotApplicableCodec extends LdbcDateCodec
    {
        @Override
        public Resolution resolution()
        {
            return Resolution.NOT_APPLICABLE;
        }

        @Override
        public long utcToEncodedDateAtResolution( long utc, Calendar calendar )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }

        @Override
        public long encodedDateTimeToEncodedDateAtResolution( long encodedDateTime )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }

        @Override
        public String encodedDateAtResolutionToString( long encodedDateTimeAtResolution )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }

        @Override
        public void populateCalendarFromEncodedDateAtResolution( long encodedDateAtResolution, Calendar calendar )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }

        @Override
        public void incrementCalendarByTimestampResolution( Calendar calendar, int increment )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }

        @Override
        public long calendarToEncodedDateAtResolution( Calendar calendar )
        {
            throw new UnsupportedOperationException( "Should never get called" );
        }
    }
}
