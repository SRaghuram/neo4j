/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class QueryDateUtilTest
{
    /*
    UTC
     */

    @Test
    public void shouldCorrectlyConvertUtcAndYearTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.YEAR );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 1982L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndMonthTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.MONTH );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 198201L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndDayTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.DAY );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 19820123L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndHourTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.HOUR );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 1982012301L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndMinuteTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.MINUTE );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 198201230102L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndSecondTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.SECOND );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 19820123010203L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertUtcAndMilliSecondTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_UTC,
                LdbcDateCodec.Resolution.MILLISECOND );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_UTC ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( utcDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( utcDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( utcDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( utcDateTime ), equalTo( 23 ) );
    }

    /*
    Encoded Long
     */

    @Test
    public void shouldCorrectlyConvertEncodedLongAndYearTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.YEAR );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 1982L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndMonthTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 198201L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndDayTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.DAY );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 19820123L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndHourTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 1982012301L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndMinuteTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MINUTE );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 198201230102L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndSecondTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.SECOND );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ), equalTo( 19820123010203L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }

    @Test
    public void shouldCorrectlyConvertEncodedLongAndMilliSecondTimestamp() throws ParseException
    {
        Calendar calendar = LdbcDateCodec.newCalendar();
        calendar.set( Calendar.YEAR, 1982 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 23 );
        calendar.set( Calendar.HOUR_OF_DAY, 1 );
        calendar.set( Calendar.MINUTE, 2 );
        calendar.set( Calendar.SECOND, 3 );
        calendar.set( Calendar.MILLISECOND, 4 );

        long utcDateTime = calendar.getTimeInMillis();
        long encodedLongDateTime = 19820123010203004L;

        QueryDateUtil queryDateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MILLISECOND );

        assertThat( queryDateUtil.dateFormat(), equalTo( LdbcDateCodec.Format.NUMBER_ENCODED ) );
        assertThat( queryDateUtil.utcToFormat( utcDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateAtResolution( encodedLongDateTime ),
                    equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToEncodedDateTime( encodedLongDateTime ), equalTo( 19820123010203004L ) );
        assertThat( queryDateUtil.formatToUtc( encodedLongDateTime ), equalTo( utcDateTime ) );
        assertThat( queryDateUtil.formatToYear( encodedLongDateTime ), equalTo( 1982 ) );
        assertThat( queryDateUtil.formatToMonth( encodedLongDateTime ), equalTo( 1 ) );
        assertThat( queryDateUtil.formatToDay( encodedLongDateTime ), equalTo( 23 ) );
    }
}
