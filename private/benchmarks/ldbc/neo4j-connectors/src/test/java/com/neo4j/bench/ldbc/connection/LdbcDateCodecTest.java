/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Calendar;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LdbcDateCodecTest
{
    private static final LdbcDateCodecUtil LDBC_DATE_CODEC_UTIL = new LdbcDateCodecUtil();

    // ----- Static -----

    @Test
    public void shouldCorrectlyConvertEncodedDateStringToUtc() throws ParseException
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );
        long utcDate = calendar.getTimeInMillis();

        String encodedStringDate = LDBC_DATE_CODEC_UTIL.utcToEncodedDateString( utcDateTime );

        assertThat(
                encodedStringDate,
                equalTo( "1982-01-23" ) );
        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedDateStringToUtc( encodedStringDate ),
                equalTo( utcDate ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedDateTimeStringToUtc() throws ParseException
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDate = calendar.getTimeInMillis();

        String encodedStringDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedDateTimeString( utcDate );

        assertThat(
                encodedStringDateTime,
                equalTo( "1982-01-23T01:02:03.004+0000" ) );
        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedDateTimeStringToUtc( encodedStringDateTime ),
                equalTo( utcDate ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedDateStringToEncodedLong() throws ParseException
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );
        long utcDate = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        String encodedStringDate = LDBC_DATE_CODEC_UTIL.utcToEncodedDateString( utcDate );

        assertThat(
                encodedStringDate,
                equalTo( "1982-01-23" ) );
        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedDateStringToEncodedLongDateTime( encodedStringDate, calendar ),
                equalTo( 19820123000000000L ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedDateTimeStringToEncodedLong() throws ParseException
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        String encodedStringDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedDateTimeString( utcDateTime );

        assertThat(
                encodedStringDateTime,
                equalTo( "1982-01-23T01:02:03.004+0000" ) );
        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedDateTimeStringToEncodedLongDateTime( encodedStringDateTime, calendar ),
                equalTo( 19820123010203004L ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcToEncodedDateString()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();

        String encodedStringDate = LDBC_DATE_CODEC_UTIL.utcToEncodedDateString( utcDateTime );

        assertThat(
                encodedStringDate,
                equalTo( "1982-01-23" ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcToEncodedDateTimeString()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();

        String encodedStringDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedDateTimeString( utcDateTime );

        assertThat(
                encodedStringDateTime,
                equalTo( "1982-01-23T01:02:03.004+0000" ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcToEncodedLong()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();
        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongDateTimeToEncodedLongDate()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongToEncodedDateString()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );

        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedLongDateTimeToEncodedDateString( encodedLongDateTime, calendar ),
                equalTo( "1982-01-23" ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongToEncodedDateTimeString()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );

        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedLongDateTimeToEncodedDateTimeString( encodedLongDateTime, calendar ),
                equalTo( "1982-01-23T01:02:03.004+0000" ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongToUtc()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );

        assertThat(
                LDBC_DATE_CODEC_UTIL.encodedLongDateTimeToUtc( encodedLongDateTime, calendar ),
                equalTo( utcDateTime ) );
    }

    @Test
    public void shouldCorrectlyPopulateCalendarFromEncodedLongDate()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );
        long utcDateTime = calendar.getTimeInMillis();
        calendar = LdbcDateCodecUtil.newCalendar();

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedLongDateTime( utcDateTime, calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );

        Calendar calendarAfter = LdbcDateCodecUtil.newCalendar();
        LDBC_DATE_CODEC_UTIL.populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendarAfter );

        assertThat( utcDateTime, equalTo( calendarAfter.getTimeInMillis() ) );
    }

    @Test
    public void shouldCorrectlyCreateEncodedLongDateFromCalendar()
    {
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long encodedLongDateTime = LDBC_DATE_CODEC_UTIL.calendarToEncodedLongDateTime( calendar );

        assertThat(
                encodedLongDateTime,
                equalTo( 19820123010203004L ) );
    }

    // ----- Instance -----

    @Test
    public void shouldCorrectlyDoYearResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.YEAR );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.YEAR ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 1982L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "1982" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 1982L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 1982L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 1978L, 1982L, calendar ),
                equalTo( new long[]{1978L, 1979L, 1980L, 1981L, 1982L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 1982L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 1982L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 1984L ) );
    }

    @Test
    public void shouldCorrectlyDoMonthResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.MONTH );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.MONTH ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 198201L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "198201" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 198201L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 198201L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 198201L, 198204L, calendar ),
                equalTo( new long[]{198201L, 198202L, 198203L, 198204L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 198201L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 198201L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 198203L ) );
    }

    @Test
    public void shouldCorrectlyDoDayResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.DAY );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.DAY ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 19820123L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "19820123" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 19820123L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 19820227L, 19820301L, calendar ),
                equalTo( new long[]{19820227L, 19820228L, 19820301L} )
        );
        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 19840227L, 19840301L, calendar ),
                equalTo( new long[]{19840227L, 19840228L, 19840229L, 19840301L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820123L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820125L ) );
    }

    @Test
    public void shouldCorrectlyDoHourResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.HOUR );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.HOUR ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 1982012301L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "1982012301" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 1982012301L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 1982012301L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 1982012300L, 1982012301L, calendar ),
                equalTo( new long[]{1982012300L, 1982012301L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 1982012323L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 1982012323L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 1982012401L ) );
    }

    @Test
    public void shouldCorrectlyDoMinuteResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.MINUTE );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.MINUTE ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 198201230102L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "198201230102" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ),
                    equalTo( 198201230102L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 198201230102L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 198201230058L, 198201230101L, calendar ),
                equalTo( new long[]{198201230058L, 198201230059L, 198201230100L, 198201230101L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 198201230058L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 198201230058L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 198201230100L ) );
    }

    @Test
    public void shouldCorrectlyDoSecondResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.SECOND );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.SECOND ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 19820123010203L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "19820123010203" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ),
                    equalTo( 19820123010203L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123010203L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 19820123005859L, 19820123005900L, calendar ),
                equalTo( new long[]{19820123005859L, 19820123005900L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123005859L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820123005859L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820123005901L ) );
    }

    @Test
    public void shouldCorrectlyDoMilliSecondResolution() throws ParseException
    {
        LdbcDateCodec dateCodec = LdbcDateCodec.codecFor( LdbcDateCodec.Resolution.MILLISECOND );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        assertThat( dateCodec.resolution(), equalTo( LdbcDateCodec.Resolution.MILLISECOND ) );
        assertThat( dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar ), equalTo( 19820123010203004L ) );
        assertThat(
                dateCodec.encodedDateAtResolutionToString(
                        dateCodec.utcToEncodedDateAtResolution( utcDateTime, calendar )
                ),
                equalTo( "19820123010203004" ) );
        assertThat( dateCodec.encodedDateTimeToEncodedDateAtResolution( encodedLongDateTime ),
                    equalTo( 19820123010203004L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123010203004L, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        assertThat(
                dateCodec.encodedDatesAtResolutionForRange( 19820123005859998L, 19820123005900001L, calendar ),
                equalTo( new long[]{19820123005859998L, 19820123005859999L, 19820123005900000L, 19820123005900001L} )
        );

        calendar = LdbcDateCodecUtil.newCalendar();
        dateCodec.populateCalendarFromEncodedDateAtResolution( 19820123005900001L, calendar );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820123005900001L ) );
        dateCodec.incrementCalendarByTimestampResolution( calendar, 2 );
        assertThat( dateCodec.calendarToEncodedDateAtResolution( calendar ), equalTo( 19820123005900003L ) );
    }
}
