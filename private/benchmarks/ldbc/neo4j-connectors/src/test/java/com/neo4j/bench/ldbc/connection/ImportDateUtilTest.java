/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Calendar;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ImportDateUtilTest
{
    private static final LdbcDateCodecUtil LDBC_DATE_CODEC_UTIL = new LdbcDateCodecUtil();

    @Test
    public void shouldNotCreateUtcToEncodedStringConverter() throws ParseException
    {
        assertThrows( RuntimeException.class, () ->
        {
            ImportDateUtil.createFor(
                    LdbcDateCodec.Format.NUMBER_UTC,
                    LdbcDateCodec.Format.STRING_ENCODED,
                    LdbcDateCodec.Resolution.HOUR );
        });
    }

    @Test
    public void shouldNotCreateEncodedNumberToEncodedStringConverter() throws ParseException
    {
        assertThrows( RuntimeException.class, () ->
        {
            ImportDateUtil.createFor(
                    LdbcDateCodec.Format.NUMBER_ENCODED,
                    LdbcDateCodec.Format.STRING_ENCODED,
                    LdbcDateCodec.Resolution.HOUR );
        });
    }

    @Test
    public void shouldNotCreateEncodedStringToEncodedStringConverter() throws ParseException
    {
        assertThrows( RuntimeException.class, () ->
        {
            ImportDateUtil.createFor(
                    LdbcDateCodec.Format.STRING_ENCODED,
                    LdbcDateCodec.Format.STRING_ENCODED,
                    LdbcDateCodec.Resolution.HOUR );
        });
    }

    @Test
    public void shouldCorrectlyConvertEncodedStringToUtc() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.YEAR );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );

        long utcDate = calendar.getTimeInMillis();

        String encodedStringDate = LDBC_DATE_CODEC_UTIL.utcToEncodedDateString( utcDateTime );
        String encodedStringDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedDateTimeString( utcDateTime );

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.STRING_ENCODED ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat(
                importDateUtil.csvDateToFormat( encodedStringDate, calendar ),
                equalTo( utcDate ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( encodedStringDateTime, calendar ),
                equalTo( utcDateTime ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( encodedStringDate, calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( encodedStringDateTime, calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( encodedStringDate, calendar ),
                equalTo( 1982L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( encodedStringDateTime, calendar ),
                equalTo( 1982L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( encodedStringDate, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( encodedStringDateTime, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( calendar.getTimeInMillis() ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedStringToEncodedLong() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.STRING_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH );

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
        String encodedStringDateTime = LDBC_DATE_CODEC_UTIL.utcToEncodedDateTimeString( utcDateTime );
        assertThat( encodedStringDate, equalTo( "1982-01-23" ) );
        assertThat( encodedStringDateTime, equalTo( "1982-01-23T01:02:03.004+0000" ) );

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.STRING_ENCODED ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat(
                importDateUtil.csvDateToFormat( encodedStringDate, calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( encodedStringDateTime, calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( encodedStringDate, calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( encodedStringDateTime, calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( encodedStringDate, calendar ),
                equalTo( 198201L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( encodedStringDateTime, calendar ),
                equalTo( 198201L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( encodedStringDate, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123000000000L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( encodedStringDateTime, calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123010203004L ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcToUtc() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.DAY );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );

        long utcDate = calendar.getTimeInMillis();

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat(
                importDateUtil.csvDateToFormat( Long.toString( utcDate ), calendar ),
                equalTo( utcDate ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( Long.toString( utcDateTime ), calendar ),
                equalTo( utcDateTime ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( Long.toString( utcDate ), calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( Long.toString( utcDateTime ), calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( Long.toString( utcDate ), calendar ),
                equalTo( 19820123L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( Long.toString( utcDateTime ), calendar ),
                equalTo( 19820123L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( Long.toString( utcDate ), calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( calendar.getTimeInMillis() ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( Long.toString( utcDateTime ), calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( calendar.getTimeInMillis() ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcToEncodedLong() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );

        long utcDate = calendar.getTimeInMillis();

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat(
                importDateUtil.csvDateToFormat( Long.toString( utcDate ), calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( Long.toString( utcDateTime ), calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( Long.toString( utcDate ), calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( Long.toString( utcDateTime ), calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( Long.toString( utcDate ), calendar ),
                equalTo( 1982012300L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( Long.toString( utcDateTime ), calendar ),
                equalTo( 1982012301L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( Long.toString( utcDate ), calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123000000000L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( Long.toString( utcDateTime ), calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123010203004L ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongToUtc() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.MINUTE );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        calendar.set( Calendar.HOUR_OF_DAY, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MILLISECOND, 0 );

        long utcDate = calendar.getTimeInMillis();

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat(
                importDateUtil.csvDateToFormat( "19820123000000000", calendar ),
                equalTo( utcDate ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( "19820123010203004", calendar ),
                equalTo( utcDateTime ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( "19820123000000000", calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( "19820123010203004", calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( "19820123000000000", calendar ),
                equalTo( 198201230000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( "19820123010203004", calendar ),
                equalTo( 198201230102L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( "19820123000000000", calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( calendar.getTimeInMillis() ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( "19820123010203004", calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( calendar.getTimeInMillis() ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongToEncodedLong() throws ParseException
    {
        ImportDateUtil importDateUtil = ImportDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.SECOND );

        Calendar calendar = LdbcDateCodecUtil.newCalendar();

        assertThat( importDateUtil.fromFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( importDateUtil.toFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat(
                importDateUtil.csvDateToFormat( "19820123000000000", calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToFormat( "19820123010203004", calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateTime( "19820123000000000", calendar ),
                equalTo( 19820123000000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateTime( "19820123010203004", calendar ),
                equalTo( 19820123010203004L ) );
        assertThat(
                importDateUtil.csvDateToEncodedDateAtResolution( "19820123000000000", calendar ),
                equalTo( 19820123000000L ) );
        assertThat(
                importDateUtil.csvDateTimeToEncodedDateAtResolution( "19820123010203004", calendar ),
                equalTo( 19820123010203L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDate( "19820123000000000", calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 0 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 0 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123000000000L ) );

        calendar = LdbcDateCodecUtil.newCalendar();
        importDateUtil.populateCalendarFromCsvDateTime( "19820123010203004", calendar );
        assertThat( calendar.get( Calendar.YEAR ), equalTo( 1982 ) );
        assertThat( calendar.get( Calendar.MONTH ), equalTo( Calendar.JANUARY ) );
        assertThat( calendar.get( Calendar.DAY_OF_MONTH ), equalTo( 23 ) );
        assertThat( calendar.get( Calendar.HOUR_OF_DAY ), equalTo( 1 ) );
        assertThat( calendar.get( Calendar.MINUTE ), equalTo( 2 ) );
        assertThat( calendar.get( Calendar.SECOND ), equalTo( 3 ) );
        assertThat( calendar.get( Calendar.MILLISECOND ), equalTo( 4 ) );

        assertThat( importDateUtil.calendarToFormat( calendar ), equalTo( 19820123010203004L ) );
    }
}
