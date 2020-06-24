/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Domain.Rels;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Calendar;

import org.neo4j.graphdb.RelationshipType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeStampedRelationshipTypesCacheTest
{
    @Test
    public void shouldDoJoinArrays() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        RelationshipType[] actualRange1 = cache.commentHasCreatorForDateRange(
                calendar,
                1982012300L,
                1982012300L,
                dateUtil );
        RelationshipType[] expectedRange1 = new RelationshipType[]{
                RelationshipType.withName(
                        Rels.COMMENT_HAS_CREATOR + Long.toString( 1982012300L ) )
        };

        for ( int i = 0; i < actualRange1.length; i++ )
        {
            assertThat( actualRange1[i].name(), equalTo( expectedRange1[i].name() ) );
        }

        RelationshipType[] actualRange2 = cache.postHasCreatorForDateRange(
                calendar,
                1982012300L,
                1982012301L,
                dateUtil );
        RelationshipType[] expectedRange2 = new RelationshipType[]{
                RelationshipType.withName(
                        Rels.POST_HAS_CREATOR + Long.toString( 1982012300L ) ),
                RelationshipType.withName(
                        Rels.POST_HAS_CREATOR + Long.toString( 1982012301L ) )
        };

        for ( int i = 0; i < actualRange2.length; i++ )
        {
            assertThat( actualRange2[i].name(), equalTo( expectedRange2[i].name() ) );
        }

        RelationshipType[] actualRange3 = TimeStampedRelationshipTypesCache.joinArrays( actualRange1, actualRange2 );
        RelationshipType[] expectedRange3 = new RelationshipType[]{
                RelationshipType.withName(
                        Rels.COMMENT_HAS_CREATOR + Long.toString( 1982012300L ) ),
                RelationshipType.withName(
                        Rels.POST_HAS_CREATOR + Long.toString( 1982012300L ) ),
                RelationshipType.withName(
                        Rels.POST_HAS_CREATOR + Long.toString( 1982012301L ) )
        };

        for ( int i = 0; i < actualRange3.length; i++ )
        {
            assertThat( actualRange3[i].name(), equalTo( expectedRange3[i].name() ) );
        }
    }

    // ===================
    // Comment Has Creator
    // ===================

    @Test
    public void shouldFailCommentHasCreatorForDateRangeForInvalid() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        long minDate = 1982012301L;
        long maxDate = 1982012300L;

        assertThrows( DbException.class, () ->
        {
                cache.commentHasCreatorForDateRange(
                        calendar,
                        minDate,
                        maxDate,
                        dateUtil );
        });
    }

    @Test
    public void shouldGetCommentHasCreatorForDateRangeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        minDate = 1982012300L;
        maxDate = 1982012400L;
        range = cache.commentHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        minDate = 1982012300L;
        maxDate = 1982012300L;
        range = cache.commentHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        range = cache.commentHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198201L;
        maxDate = 198201L;
        range = cache.commentHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );
    }

    @Test
    // resizeCommentHasCreatorForNewDate
    // commentHasCreatorForAllDates
    public void shouldGetCommentHasCreatorForAllDatesOnValidInputs() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar;
        QueryDateUtil dateUtil;
        RelationshipType[] range;
        long nextVal;
        long minDate;
        long maxDate;
        boolean wasResized;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize high end
        nextVal = 1982012301L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012301L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize low end
        nextVal = 1982012223L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 198201;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize high end
        nextVal = 198202L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize low end
        nextVal = 198112L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_HAS_CREATOR.name() + maxDate ) );
    }

    @Test
    public void shouldDoCommentHasCreatorForDateAtResolution()
    {
        TimeStampedRelationshipTypesCache cache;
        QueryDateUtil dateUtil;
        long dateAtResolution;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        dateAtResolution = 1982012301L;

        assertThat(
                cache.commentHasCreatorForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.COMMENT_HAS_CREATOR + Long.toString( dateAtResolution ) )
                  );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        dateAtResolution = 198201L;

        assertThat(
                cache.commentHasCreatorForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.COMMENT_HAS_CREATOR + Long.toString( dateAtResolution ) )
                  );
    }

    // ===================
    // Post Has Creator
    // ===================

    @Test
    public void shouldFailPostHasCreatorForDateRangeWithInvalid() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        long minDate = 1982012301L;
        long maxDate = 1982012300L;

        assertThrows( DbException.class, () ->
        {
            cache.postHasCreatorForDateRange(
                    calendar,
                    minDate,
                    maxDate,
                    dateUtil );
        });
    }

    @Test
    public void shouldGetPostHasCreatorForDateRangeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        minDate = 1982012300L;
        maxDate = 1982012400L;
        range = cache.postHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        minDate = 1982012300L;
        maxDate = 1982012300L;
        range = cache.postHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        range = cache.postHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198201L;
        maxDate = 198201L;
        range = cache.postHasCreatorForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );
    }

    @Test
    // resizePostHasCreatorForNewDate
    // postHasCreatorForAllDates
    public void shouldGetPostHasCreatorForAllDatesOnValidInputs() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar;
        QueryDateUtil dateUtil;
        RelationshipType[] range;
        long nextVal;
        long minDate;
        long maxDate;
        boolean wasResized;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 1982012300L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize high end
        nextVal = 1982012301L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012301L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize low end
        nextVal = 1982012223L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 198201;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize high end
        nextVal = 198202L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize low end
        nextVal = 198112L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostHasCreatorForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postHasCreatorForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_HAS_CREATOR.name() + maxDate ) );
    }

    @Test
    public void shouldDoPostHasCreatorForDateAtResolution()
    {
        TimeStampedRelationshipTypesCache cache;
        QueryDateUtil dateUtil;
        long dateAtResolution;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        dateAtResolution = 1982012301L;

        assertThat(
                cache.postHasCreatorForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.POST_HAS_CREATOR + Long.toString( dateAtResolution ) )
                  );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        dateAtResolution = 198201L;

        assertThat(
                cache.postHasCreatorForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.POST_HAS_CREATOR + Long.toString( dateAtResolution ) )
                  );
    }

    // ===================
    // Post Is Located In
    // ===================

    @Test
    public void shouldFailPostIsLocatedInForDateRangeForInvalid() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        long minDate = 1982012301L;
        long maxDate = 1982012300L;

        assertThrows( DbException.class, () ->
        {
            cache.postIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        });
    }

    @Test
    public void shouldGetPostIsLocatedInForDateRangeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        minDate = 1982012300L;
        maxDate = 1982012400L;
        range = cache.postIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        minDate = 1982012300L;
        maxDate = 1982012300L;
        range = cache.postIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        range = cache.postIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198201L;
        maxDate = 198201L;
        range = cache.postIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );
    }

    @Test
    // resizePostIsLocatedInForNewDate
    // postIsLocatedInForAllDates
    public void shouldGetPostIsLocatedInForAllDatesOnValidInputs() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar;
        QueryDateUtil dateUtil;
        RelationshipType[] range;
        long nextVal;
        long minDate;
        long maxDate;
        boolean wasResized;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 1982012300L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize high end
        nextVal = 1982012301L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012301L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize low end
        nextVal = 1982012223L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 198201;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize high end
        nextVal = 198202L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize low end
        nextVal = 198112L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizePostIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.postIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.POST_IS_LOCATED_IN.name() + maxDate ) );
    }

    @Test
    public void shouldDoPostIsLocatedInForDateAtResolution()
    {
        TimeStampedRelationshipTypesCache cache;
        QueryDateUtil dateUtil;
        long dateAtResolution;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        dateAtResolution = 1982012301L;

        assertThat(
                cache.postIsLocatedInForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.POST_IS_LOCATED_IN + Long.toString( dateAtResolution ) )
                  );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        dateAtResolution = 198201L;

        assertThat(
                cache.postIsLocatedInForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.POST_IS_LOCATED_IN + Long.toString( dateAtResolution ) )
                  );
    }

    // =====================
    // Comment Is Located In
    // =====================

    @Test
    public void shouldFailCommentIsLocatedInForDateRangeForInvalid() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        long minDate = 1982012301L;
        long maxDate = 1982012300L;

        assertThrows( DbException.class, () ->
        {
            cache.commentIsLocatedInForDateRange(
                    calendar,
                    minDate,
                    maxDate,
                    dateUtil );
        });
    }

    @Test
    public void shouldGetCommentIsLocatedInForDateRangeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        minDate = 1982012300L;
        maxDate = 1982012400L;
        range = cache.commentIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        minDate = 1982012300L;
        maxDate = 1982012300L;
        range = cache.commentIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        range = cache.commentIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198201L;
        maxDate = 198201L;
        range = cache.commentIsLocatedInForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );
    }

    @Test
    // resizeCommentIsLocatedInForNewDate
    // commentIsLocatedInForAllDates
    public void shouldGetCommentIsLocatedInForAllDatesOnValidInputs() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar;
        QueryDateUtil dateUtil;
        RelationshipType[] range;
        long nextVal;
        long minDate;
        long maxDate;
        boolean wasResized;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize high end
        nextVal = 1982012301L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012301L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize low end
        nextVal = 1982012223L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 198201;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize high end
        nextVal = 198202L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize low end
        nextVal = 198112L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeCommentIsLocatedInForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.commentIsLocatedInForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN.name() + maxDate ) );
    }

    @Test
    public void shouldDoCommentIsLocatedInForDateAtResolution()
    {
        TimeStampedRelationshipTypesCache cache;
        QueryDateUtil dateUtil;
        long dateAtResolution;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        dateAtResolution = 1982012301L;

        assertThat(
                cache.commentIsLocatedInForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN + Long.toString( dateAtResolution ) )
                  );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        dateAtResolution = 198201L;

        assertThat(
                cache.commentIsLocatedInForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.COMMENT_IS_LOCATED_IN + Long.toString( dateAtResolution ) )
                  );
    }

    // ===================
    // Has Member
    // ===================

    @Test
    public void shouldFailHasMemberForDateRangeForInvalid() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        long minDate = 1982012301L;
        long maxDate = 1982012300L;

        assertThrows( DbException.class, () ->
        {
            cache.hasMemberForDateRange(
                    calendar,
                    minDate,
                    maxDate,
                    dateUtil );
        });
    }

    @Test
    public void shouldFailHasMemberForDatesAfterIfNotInitialized() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        assertThrows( DbException.class, () ->
        {
            cache.hasMemberForDatesAfter( 1982012301L );
        });
    }

    @Test
    public void shouldFailHasMemberForDatesAfterIfBelowRange() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        cache.resizeHasMemberForNewDate( 1982012302L, calendar, dateUtil );

        assertThrows( DbException.class, () ->
        {
            cache.hasMemberForDatesAfter( 1982012301L );
        });
    }

    // TODO remove ignore
    // TODO at present param gen creates params out of range, so cache returns empty array instead or error
    @Disabled
    @Test
    public void shouldFailHasMemberForDatesAfterIfAboveRange() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        QueryDateUtil dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        cache.resizeHasMemberForNewDate( 1982012301L, calendar, dateUtil );
        assertThrows( DbException.class, () ->
        {
        cache.hasMemberForDatesAfter( 1982012302L );
        });
    }

    @Test
    public void shouldGetHasMemberForDatesAfterForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = 1982012300L;
        maxDate = 1982012400L;
        cache.resizeHasMemberForNewDate( minDate, calendar, dateUtil );
        cache.resizeHasMemberForNewDate( maxDate, calendar, dateUtil );

        range = cache.hasMemberForDatesAfter( maxDate );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        range = cache.hasMemberForDatesAfter( 1982012322L );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + 1982012322L ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( Arrays.toString( range ), range.length, equalTo( 3 ) );

        minDate = 1982012300L;
        range = cache.hasMemberForDatesAfter( minDate );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        cache.resizeHasMemberForNewDate( minDate, calendar, dateUtil );
        cache.resizeHasMemberForNewDate( maxDate, calendar, dateUtil );
        range = cache.hasMemberForDatesAfter( minDate );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198211L;
        range = cache.hasMemberForDatesAfter( minDate );
        assertThat( Arrays.toString( range ),
                    range[0].name(),
                    equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 2 ) );
    }

    @Test
    public void shouldGetHasMemberForDateRangeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar = LdbcDateCodecUtil.newCalendar();
        RelationshipType[] range;
        long minDate;
        long maxDate;
        QueryDateUtil dateUtil;

        // Hour Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );

        minDate = 1982012300L;
        maxDate = 1982012400L;
        range = cache.hasMemberForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 25 ) );

        minDate = 1982012300L;
        maxDate = 1982012300L;
        range = cache.hasMemberForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );

        // Month Resolution

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );

        minDate = 198201L;
        maxDate = 198212L;
        range = cache.hasMemberForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 12 ) );

        minDate = 198201L;
        maxDate = 198201L;
        range = cache.hasMemberForDateRange(
                calendar,
                minDate,
                maxDate,
                dateUtil );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
        assertThat( range.length, equalTo( 1 ) );
    }

    @Test
    // resizeHasMemberForNewDate
    // hasMemberForAllDates
    public void shouldGetHasMemberForAllDatesOnValidInputs() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        Calendar calendar;
        QueryDateUtil dateUtil;
        RelationshipType[] range;
        long nextVal;
        long minDate;
        long maxDate;
        boolean wasResized;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.hasMemberForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 1982012300L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012300L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize high end
        nextVal = 1982012301L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012301L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012300L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize low end
        nextVal = 1982012223L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 1982012300L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 1982012223L ) );
        assertThat( maxDate, equalTo( 1982012301L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        calendar = LdbcDateCodecUtil.newCalendar();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        minDate = Long.MAX_VALUE;
        maxDate = Long.MIN_VALUE;

        // Initial state
        range = cache.hasMemberForAllDates();
        assertThat( range.length, is( 0 ) );

        // First resize
        nextVal = 198201;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198201L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 1 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize high end
        nextVal = 198202L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198201L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 2 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize low end
        nextVal = 198112L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( true ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );

        // Resize within existing range --> no resize should occur
        nextVal = 198201L;
        wasResized = cache.resizeHasMemberForNewDate( nextVal, calendar, dateUtil );
        assertThat( wasResized, equalTo( false ) );
        minDate = Math.min( minDate, nextVal );
        maxDate = Math.max( maxDate, nextVal );
        assertThat( minDate, equalTo( 198112L ) );
        assertThat( maxDate, equalTo( 198202L ) );
        range = cache.hasMemberForAllDates();
        assertThat( range.length, equalTo( 3 ) );
        assertThat(
                range[0].name(),
                equalTo( Rels.HAS_MEMBER.name() + minDate ) );
        assertThat(
                range[range.length - 1].name(),
                equalTo( Rels.HAS_MEMBER.name() + maxDate ) );
    }

    @Test
    public void shouldDoHasMemberForDateAtResolution()
    {
        TimeStampedRelationshipTypesCache cache;
        QueryDateUtil dateUtil;
        long dateAtResolution;

        // ===========================
        // ===== Hour Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.HOUR,
                new LdbcDateCodecUtil() );
        dateAtResolution = 1982012301L;

        assertThat(
                cache.hasMemberForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.HAS_MEMBER + Long.toString( dateAtResolution ) )
                  );

        // ===========================
        // ===== Month Resolution =====
        // ===========================

        cache = new TimeStampedRelationshipTypesCache();
        dateUtil = QueryDateUtil.createFor(
                LdbcDateCodec.Format.NUMBER_ENCODED,
                LdbcDateCodec.Resolution.MONTH,
                new LdbcDateCodecUtil() );
        dateAtResolution = 198201L;

        assertThat(
                cache.hasMemberForDateAtResolution( dateAtResolution, dateUtil ).name(),
                equalTo( Rels.HAS_MEMBER + Long.toString( dateAtResolution ) )
                  );
    }

    // =========
    // Works At
    // =========

    @Test
    public void shouldFailWorksAtForYearsBeforeForInvalidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        int year;

        cache = new TimeStampedRelationshipTypesCache();
        year = 1982;

        assertThrows( DbException.class, () ->
        {
            cache.worksAtForYearsBefore( year );
        });
    }

    @Test
    public void shouldGetWorksAtForYearsBeforeForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        RelationshipType[] actualRange;
        RelationshipType[] expectedRange;
        int maxYear;

        cache = new TimeStampedRelationshipTypesCache();

        // Initialize, must be resized at least once before initial call
        maxYear = 1982;
        assertThat( cache.resizeWorksAtForNewYear( maxYear ), equalTo( true ) );

        // Single value
        maxYear = 1982;
        actualRange = cache.worksAtForYearsBefore( maxYear );
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Increase at high end
        maxYear = 1984;
        actualRange = cache.worksAtForYearsBefore( maxYear );
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1984 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Within existing range
        maxYear = 1983;
        actualRange = cache.worksAtForYearsBefore( maxYear );
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Increase at low end beyond minimum
        boolean exceptionThrown = false;
        maxYear = 1981;
        try
        {
            cache.worksAtForYearsBefore( maxYear );
        }
        catch ( DbException dbException )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, equalTo( true ) );
    }

    @Test
    public void shouldFailWorksAtForAllYearsForInvalidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;

        cache = new TimeStampedRelationshipTypesCache();

        // Has not been initialized
        assertThrows( DbException.class, () ->
        {
            cache.worksAtForAllYears();
        });
    }

    @Test
    public void shouldGetWorksAtForAllYearsForValidInput() throws DbException
    {
        TimeStampedRelationshipTypesCache cache;
        RelationshipType[] actualRange;
        RelationshipType[] expectedRange;
        int year;

        cache = new TimeStampedRelationshipTypesCache();

        // Initialize, must be resized at least once before initial call
        year = 1982;
        assertThat( cache.resizeWorksAtForNewYear( year ), equalTo( true ) );

        // Single value
        actualRange = cache.worksAtForAllYears();
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Should not resize
        year = 1982;
        assertThat( cache.resizeWorksAtForNewYear( year ), equalTo( false ) );

        // Resize upper end
        year = 1984;
        assertThat( cache.resizeWorksAtForNewYear( year ), equalTo( true ) );
        actualRange = cache.worksAtForAllYears();
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1984 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Without resize -- same again
        actualRange = cache.worksAtForAllYears();
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1984 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Resize in existing range -- nothing should change
        year = 1983;
        assertThat( cache.resizeWorksAtForNewYear( year ), equalTo( false ) );
        actualRange = cache.worksAtForAllYears();
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1984 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }

        // Resize lower end
        year = 1981;
        assertThat( cache.resizeWorksAtForNewYear( year ), equalTo( true ) );
        actualRange = cache.worksAtForAllYears();
        expectedRange = new RelationshipType[]{
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1981 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1982 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1983 ) ),
                RelationshipType.withName( Rels.WORKS_AT + Integer.toString( 1984 ) )
        };
        for ( int i = 0; i < actualRange.length; i++ )
        {
            assertThat( actualRange[i].name(), equalTo( expectedRange[i].name() ) );
        }
    }

    @Test
    public void shouldResizeWorksAtForNewYear() throws DbException
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        RelationshipType[] range;

        // Initial (re)sizing
        assertThat( cache.resizeWorksAtForNewYear( 1982 ), equalTo( true ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize within existing range -- nothing should change
        assertThat( cache.resizeWorksAtForNewYear( 1982 ), equalTo( false ) );
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize lower end
        assertThat( cache.resizeWorksAtForNewYear( 1981 ), equalTo( true ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1981 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize lower end
        assertThat( cache.resizeWorksAtForNewYear( 1980 ), equalTo( true ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1980 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize within existing range -- nothing should change
        assertThat( cache.resizeWorksAtForNewYear( 1980 ), equalTo( false ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1980 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize lower end
        assertThat( cache.resizeWorksAtForNewYear( 1970 ), equalTo( true ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1970 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1982 ) ) );

        // Resize upper end
        assertThat( cache.resizeWorksAtForNewYear( 1983 ), equalTo( true ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1970 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1983 ) ) );

        // Resize within existing range -- nothing should change
        assertThat( cache.resizeWorksAtForNewYear( 1980 ), equalTo( false ) );
        range = cache.worksAtForAllYears();
        assertThat( range[0].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1970 ) ) );
        assertThat( range[range.length - 1].name(), equalTo( Rels.WORKS_AT + Integer.toString( 1983 ) ) );
    }

    @Test
    public void shouldGetWorksAtForYear()
    {
        TimeStampedRelationshipTypesCache cache = new TimeStampedRelationshipTypesCache();
        assertThat( cache.worksAtForYear( 1982 ).name(), equalTo( Rels.WORKS_AT + "1982" ) );
        assertThat( cache.worksAtForYear( 1980 ).name(), equalTo( Rels.WORKS_AT + "1980" ) );
        assertThat( cache.worksAtForYear( 0 ).name(), equalTo( Rels.WORKS_AT + "0000" ) );
    }
}
