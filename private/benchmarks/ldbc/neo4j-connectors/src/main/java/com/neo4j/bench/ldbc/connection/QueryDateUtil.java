/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import java.util.Calendar;

import static java.lang.String.format;

/**
 * NOTE: due to internal Calendar instances, no implementation is thread-safe - objects can not be shared among threads
 */
public abstract class QueryDateUtil
{
    public static QueryDateUtil createFor(
            LdbcDateCodec.Format neo4jFormat,
            LdbcDateCodec.Resolution relationshipTimestampResolution,
            LdbcDateCodecUtil ldbcDateCodecUtil )
    {
        switch ( neo4jFormat )
        {
        case NUMBER_UTC:
            return new FromUtc( LdbcDateCodec.codecFor( relationshipTimestampResolution ), ldbcDateCodecUtil );
        case NUMBER_ENCODED:
            return new FromEncodedLong( LdbcDateCodec.codecFor( relationshipTimestampResolution ), ldbcDateCodecUtil );
        default:
            throw new RuntimeException( format( "Unsupported Format %s", neo4jFormat ) );
        }
    }

    /**
     * Format of dates stored in Neo4j instance
     *
     * @return
     */
    public abstract LdbcDateCodec.Format dateFormat();

    /**
     * Internal instance of date codec
     *
     * @return
     */
    public abstract LdbcDateCodec dateCodec();

    /**
     * Internal instance of date codec util
     *
     * @return
     */
    public abstract LdbcDateCodecUtil ldbcDateCodecUtil();

    /**
     * Convert UTC encoded date time to configured format
     *
     * @return
     */
    public abstract long utcToFormat( long utc );

    /**
     * Convert from configured format to number encoded date time, at configured resolution
     *
     * @return
     */
    public abstract long formatToEncodedDateAtResolution( long dateInFormat );

    /**
     * Convert from configured format to number encoded date time (i.e. yyyymmddhhmmssmmm)
     *
     * @return
     */
    public abstract long formatToEncodedDateTime( long dateInFormat );

    /**
     * Convert from configured format to UTC encoded date time
     *
     * @return
     */
    public abstract long formatToUtc( long dateInFormat );

    /**
     * Convert from configured format to year
     *
     * @return
     */
    public abstract int formatToYear( long dateInFormat );

    /**
     * Convert from configured format to month
     *
     * @return
     */
    public abstract int formatToMonth( long dateInFormat );

    /**
     * Convert from configured format to day
     *
     * @return
     */
    public abstract int formatToDay( long dateInFormat );

    // TODO add/subtract days, months, years, etc. from date

    // TODO calculate number of days, months, years, etc. between dates

    private static class FromUtc extends QueryDateUtil
    {
        private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
        private final LdbcDateCodec dateCodec;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        FromUtc( LdbcDateCodec dateCodec, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.dateCodec = dateCodec;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format dateFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec dateCodec()
        {
            return dateCodec;
        }

        @Override
        public LdbcDateCodecUtil ldbcDateCodecUtil()
        {
            return ldbcDateCodecUtil;
        }

        @Override
        public long utcToFormat( long utc )
        {
            return utc;
        }

        @Override
        public long formatToEncodedDateAtResolution( long dateInFormat )
        {
            return dateCodec.utcToEncodedDateAtResolution( dateInFormat, calendar );
        }

        @Override
        public long formatToEncodedDateTime( long dateInFormat )
        {
            // YYYYMMDDhhmmssMMM
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( dateInFormat, calendar );
        }

        @Override
        public long formatToUtc( long dateInFormat )
        {
            return dateInFormat;
        }

        @Override
        public int formatToYear( long dateInFormat )
        {
            calendar.setTimeInMillis( dateInFormat );
            return calendar.get( Calendar.YEAR );
        }

        @Override
        public int formatToMonth( long dateInFormat )
        {
            calendar.setTimeInMillis( dateInFormat );
            return calendar.get( Calendar.MONTH ) + 1;
        }

        @Override
        public int formatToDay( long dateInFormat )
        {
            calendar.setTimeInMillis( dateInFormat );
            return calendar.get( Calendar.DAY_OF_MONTH );
        }
    }

    private static class FromEncodedLong extends QueryDateUtil
    {
        private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
        private final LdbcDateCodec dateCodec;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        FromEncodedLong( LdbcDateCodec dateCodec, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.dateCodec = dateCodec;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format dateFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec dateCodec()
        {
            return dateCodec;
        }

        @Override
        public LdbcDateCodecUtil ldbcDateCodecUtil()
        {
            return ldbcDateCodecUtil;
        }

        @Override
        public long utcToFormat( long utc )
        {
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( utc, calendar );
        }

        @Override
        public long formatToEncodedDateAtResolution( long dateInFormat )
        {
            return dateCodec.encodedDateTimeToEncodedDateAtResolution( dateInFormat );
        }

        @Override
        public long formatToEncodedDateTime( long dateInFormat )
        {
            // YYYYMMDDhhmmssMMM
            return dateInFormat;
        }

        @Override
        public long formatToUtc( long dateInFormat )
        {
            return ldbcDateCodecUtil.encodedLongDateTimeToUtc( dateInFormat, calendar );
        }

        @Override
        public int formatToYear( long dateInFormat )
        {
            // YYYYMMDDhhmmssMMM
            return (int) (dateInFormat / 10_000_000_000_000L);
        }

        @Override
        public int formatToMonth( long dateInFormat )
        {
            // YYYYMMDDhhmmssMMM
            return (int) ((dateInFormat % 10_000_000_000_000L) / 100_000_000_000L);
        }

        @Override
        public int formatToDay( long dateInFormat )
        {
            // YYYYMMDDhhmmssMMM
            return (int) ((dateInFormat % 100_000_000_000L) / 1000_000_000);
        }
    }
}
