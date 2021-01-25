/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import java.text.ParseException;
import java.util.Calendar;

import static java.lang.String.format;

public abstract class ImportDateUtil
{
    public static ImportDateUtil createFor(
            LdbcDateCodec.Format fromFormat,
            LdbcDateCodec.Format toFormat,
            LdbcDateCodec.Resolution timestampResolution )
    {
        LdbcDateCodecUtil ldbcDateCodecUtil = new LdbcDateCodecUtil();
        QueryDateUtil queryDateUtil = QueryDateUtil.createFor( toFormat, timestampResolution, ldbcDateCodecUtil );
        switch ( fromFormat )
        {
        case STRING_ENCODED:
            switch ( toFormat )
            {
            case NUMBER_UTC:
                return new EncodedStringToUtc( queryDateUtil, ldbcDateCodecUtil );
            case NUMBER_ENCODED:
                return new EncodedStringToEncodedLong( queryDateUtil, ldbcDateCodecUtil );
            default:
                throw new RuntimeException( format( "Unsupported Formats: %s --> %s", fromFormat, toFormat ) );
            }
        case NUMBER_UTC:
            switch ( toFormat )
            {
            case NUMBER_UTC:
                return new UtcToUtc( queryDateUtil, ldbcDateCodecUtil );
            case NUMBER_ENCODED:
                return new UtcToEncodedLong( queryDateUtil, ldbcDateCodecUtil );
            default:
                throw new RuntimeException( format( "Unsupported Formats: %s --> %s", fromFormat, toFormat ) );
            }
        case NUMBER_ENCODED:
            switch ( toFormat )
            {
            case NUMBER_UTC:
                return new EncodedLongToUtc( queryDateUtil, ldbcDateCodecUtil );
            case NUMBER_ENCODED:
                return new EncodedLongToEncodedLong( queryDateUtil, ldbcDateCodecUtil );
            default:
                throw new RuntimeException( format( "Unsupported Formats: %s --> %s", fromFormat, toFormat ) );
            }
        default:
            throw new RuntimeException( format( "Unsupported From Format %s", fromFormat ) );
        }
    }

    /**
     * Format of dates in CSV files
     *
     * @return
     */
    public abstract LdbcDateCodec.Format fromFormat();

    /**
     * Format of dates to be written to Neo4j instance
     *
     * @return
     */
    public abstract LdbcDateCodec.Format toFormat();

    /**
     * Resolution at which to timestamp select (e.g. *_HAS_CREATOR) relationships, to get around dense node-related
     * performance limitations, e.g.: COMMENT_HAS_CREATORyyyymmdd
     *
     * @return
     */
    public abstract LdbcDateCodec.Resolution timestampResolution();

    /**
     * Internal instance of query date utility
     *
     * @return
     */
    public abstract QueryDateUtil queryDateUtil();

    /**
     * Convert date from format in CSV files to desired Neo4j format
     *
     * @return
     */
    public abstract long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Convert date time from format in CSV files to desired Neo4j format
     *
     * @return
     */
    public abstract long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Convert date from format in CSV files to encoded number (i.e. yyyymmddhhmmssmmm) format
     *
     * @return
     */
    public abstract long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Convert date time from format in CSV files to encoded number (i.e. yyyymmddhhmmssmmm) format
     *
     * @return
     */
    public abstract long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Convert date from format in CSV files to fixed resolution timestamp encoding, e.g., yyyymmdd
     *
     * @return
     */
    public abstract long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Convert date time from format in CSV files to fixed resolution timestamp encoding, e.g., yyyymmdd
     *
     * @return
     */
    public abstract long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
            throws ParseException;

    /**
     * Convert contents of calendar to desired Neo4j format
     *
     * @return
     */
    public abstract long calendarToFormat( Calendar calendar ) throws ParseException;

    /**
     * Populate calendar fields according to date in CSV file
     *
     * @return
     */
    public abstract void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException;

    /**
     * Populate calendar fields according to date time in CSV file
     *
     * @return
     */
    public abstract void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException;

    private static class EncodedStringToUtc extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        EncodedStringToUtc( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.STRING_ENCODED;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateStringToUtc( csvDate );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateTimeStringToUtc( csvDate );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateTimeStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution(
                    ldbcDateCodecUtil.encodedDateStringToUtc( csvDate ),
                    calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution(
                    ldbcDateCodecUtil.encodedDateTimeStringToUtc( csvDate ),
                    calendar );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return calendar.getTimeInMillis();
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = ldbcDateCodecUtil.encodedDateStringToUtc( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = ldbcDateCodecUtil.encodedDateTimeStringToUtc( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }
    }

    private static class EncodedStringToEncodedLong extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        EncodedStringToEncodedLong( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.STRING_ENCODED;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateTimeStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedDateTimeStringToEncodedLongDateTime( csvDate, calendar );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution(
                    ldbcDateCodecUtil.encodedDateStringToUtc( csvDate ),
                    calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution(
                    ldbcDateCodecUtil.encodedDateTimeStringToUtc( csvDate ),
                    calendar );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.calendarToEncodedLongDateTime( calendar );
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = ldbcDateCodecUtil.encodedDateStringToUtc( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = ldbcDateCodecUtil.encodedDateTimeStringToUtc( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }
    }

    private static class UtcToUtc extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        UtcToUtc( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return calendar.getTimeInMillis();
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }
    }

    private static class UtcToEncodedLong extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        UtcToEncodedLong( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( utcDateTime, calendar );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( utcDateTime, calendar );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.utcToEncodedLongDateTime( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().utcToEncodedDateAtResolution( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.calendarToEncodedLongDateTime( calendar );
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long utcDateTime = Long.parseLong( csvDate );
            calendar.setTimeInMillis( utcDateTime );
        }
    }

    private static class EncodedLongToUtc extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        EncodedLongToUtc( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_UTC;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedLongDateTimeToUtc( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.encodedLongDateTimeToUtc( Long.parseLong( csvDate ), calendar );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( Long.parseLong( csvDate ) );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( Long.parseLong( csvDate ) );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return calendar.getTimeInMillis();
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long encodedLongDateTime = Long.parseLong( csvDate );
            ldbcDateCodecUtil.populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long encodedLongDateTime = Long.parseLong( csvDate );
            ldbcDateCodecUtil.populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        }
    }

    private static class EncodedLongToEncodedLong extends ImportDateUtil
    {
        private final QueryDateUtil queryDateUtil;
        private final LdbcDateCodecUtil ldbcDateCodecUtil;

        EncodedLongToEncodedLong( QueryDateUtil queryDateUtil, LdbcDateCodecUtil ldbcDateCodecUtil )
        {
            this.queryDateUtil = queryDateUtil;
            this.ldbcDateCodecUtil = ldbcDateCodecUtil;
        }

        @Override
        public LdbcDateCodec.Format fromFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec.Format toFormat()
        {
            return LdbcDateCodec.Format.NUMBER_ENCODED;
        }

        @Override
        public LdbcDateCodec.Resolution timestampResolution()
        {
            return queryDateUtil.dateCodec().resolution();
        }

        @Override
        public QueryDateUtil queryDateUtil()
        {
            return queryDateUtil;
        }

        @Override
        public long csvDateToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateTimeToFormat( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateTimeToEncodedDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            return Long.parseLong( csvDate );
        }

        @Override
        public long csvDateToEncodedDateAtResolution( String csvDate, Calendar calendar ) throws ParseException
        {
            return queryDateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( Long.parseLong( csvDate ) );
        }

        @Override
        public long csvDateTimeToEncodedDateAtResolution( String csvDate, Calendar calendar )
                throws ParseException
        {
            return queryDateUtil.dateCodec().encodedDateTimeToEncodedDateAtResolution( Long.parseLong( csvDate ) );
        }

        @Override
        public long calendarToFormat( Calendar calendar ) throws ParseException
        {
            return ldbcDateCodecUtil.calendarToEncodedLongDateTime( calendar );
        }

        @Override
        public void populateCalendarFromCsvDate( String csvDate, Calendar calendar ) throws ParseException
        {
            long encodedLongDateTime = Long.parseLong( csvDate );
            ldbcDateCodecUtil.populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        }

        @Override
        public void populateCalendarFromCsvDateTime( String csvDate, Calendar calendar ) throws ParseException
        {
            long encodedLongDateTime = Long.parseLong( csvDate );
            ldbcDateCodecUtil.populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        }
    }
}
