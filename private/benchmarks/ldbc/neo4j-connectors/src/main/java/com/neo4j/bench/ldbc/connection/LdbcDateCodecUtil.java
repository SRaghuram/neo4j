/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class LdbcDateCodecUtil
{
    private static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private final SimpleDateFormat dateTimeFormat;
    private final SimpleDateFormat dateFormat;

    public LdbcDateCodecUtil()
    {
        this.dateTimeFormat = new SimpleDateFormat( DATE_TIME_FORMAT_STRING );
        this.dateTimeFormat.setTimeZone( timeZone() );
        this.dateFormat = new SimpleDateFormat( DATE_FORMAT_STRING );
        this.dateFormat.setTimeZone( timeZone() );
    }

    public static Calendar newCalendar()
    {
        return Calendar.getInstance( timeZone() );
    }

    private static TimeZone timeZone()
    {
        return TimeZone.getTimeZone( "GMT" );
    }

    long encodedDateStringToUtc( String encodedStringDate ) throws ParseException
    {
        return dateFormat.parse( encodedStringDate ).getTime();
    }

    long encodedDateStringToEncodedLongDateTime( String encodedStringDate, Calendar calendar )
            throws ParseException
    {
        long utcDate = dateFormat.parse( encodedStringDate ).getTime();
        calendar.setTimeInMillis( utcDate );
        return calendarToEncodedLongDateTime( calendar );
    }

    long encodedDateTimeStringToUtc( String encodedStringDate ) throws ParseException
    {
        return dateTimeFormat.parse( encodedStringDate ).getTime();
    }

    long encodedDateTimeStringToEncodedLongDateTime( String encodedStringDate, Calendar calendar )
            throws ParseException
    {
        long utcDate = dateTimeFormat.parse( encodedStringDate ).getTime();
        calendar.setTimeInMillis( utcDate );
        return calendarToEncodedLongDateTime( calendar );
    }

    String utcToEncodedDateString( long utcDate )
    {
        return dateFormat.format( new Date( utcDate ) );
    }

    String utcToEncodedDateTimeString( long utcDate )
    {
        return dateTimeFormat.format( new Date( utcDate ) );
    }

    public long utcToEncodedLongDateTime( long utcDate, Calendar calendar )
    {
        calendar.setTimeInMillis( utcDate );
        return calendarToEncodedLongDateTime( calendar );
    }

    String encodedLongDateTimeToEncodedDateString( long encodedLongDateTime, Calendar calendar )
    {
        populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        return dateFormat.format( calendar.getTime() );
    }

    String encodedLongDateTimeToEncodedDateTimeString( long encodedLongDateTime, Calendar calendar )
    {
        populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        return dateTimeFormat.format( calendar.getTime() );
    }

    long encodedLongDateTimeToUtc( long encodedLongDateTime, Calendar calendar )
    {
        populateCalendarFromEncodedLongDateTime( encodedLongDateTime, calendar );
        return calendar.getTimeInMillis();
    }

    void populateCalendarFromEncodedLongDateTime( long encodedLongDateTime, Calendar calendar )
    {
        // YYYYMMDDhhmmssMMM
        int ms = (int) (encodedLongDateTime % 1000);
        int seconds = (int) ((encodedLongDateTime % 100_000L) / 1000);
        int minutes = (int) ((encodedLongDateTime % 10_000_000L) / 100_000);
        int hours = (int) ((encodedLongDateTime % 1000_000_000L) / 10_000_000);
        int days = (int) ((encodedLongDateTime % 100_000_000_000L) / 1000_000_000);
        int months = (int) ((encodedLongDateTime % 10_000_000_000_000L) / 100_000_000_000L) - 1;
        int years = (int) (encodedLongDateTime / 10_000_000_000_000L);
        calendar.set( Calendar.YEAR, years );
        calendar.set( Calendar.MONTH, months );
        calendar.set( Calendar.DAY_OF_MONTH, days );
        calendar.set( Calendar.HOUR_OF_DAY, hours );
        calendar.set( Calendar.MINUTE, minutes );
        calendar.set( Calendar.SECOND, seconds );
        calendar.set( Calendar.MILLISECOND, ms );
    }

    long calendarToEncodedLongDateTime( Calendar calendar )
    {
        // YYYYMMDDhhmmssMMM
        int ms = calendar.get( Calendar.MILLISECOND );
        int seconds = calendar.get( Calendar.SECOND );
        int minutes = calendar.get( Calendar.MINUTE );
        int hours = calendar.get( Calendar.HOUR_OF_DAY );
        int days = calendar.get( Calendar.DAY_OF_MONTH );
        int months = calendar.get( Calendar.MONTH ) + 1;
        int years = calendar.get( Calendar.YEAR );
        return
                ms +
                (seconds * 1000L) +
                (minutes * 100_000L) +
                (hours * 10_000_000L) +
                (days * 1000_000_000L) +
                (months * 100_000_000_000L) +
                (years * 10_000_000_000_000L);
    }
}
