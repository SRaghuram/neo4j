/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import com.google.common.collect.Maps;
import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Domain.Rels;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.RelationshipType;

import static java.lang.String.format;

public class TimeStampedRelationshipTypesCache
{
    private static final DecimalFormat WORKS_AT_YEAR_STRING_FORMAT = new DecimalFormat( "0000" );

    private final ConcurrentMap<Long,RelationshipType> postHasCreatorRelationshipTypeCache =
            Maps.newConcurrentMap();
    private final ConcurrentMap<Long,RelationshipType> commentHasCreatorRelationshipTypeCache =
            Maps.newConcurrentMap();
    private final ConcurrentMap<Integer,RelationshipType> worksAtRelationshipTypeCache =
            Maps.newConcurrentMap();
    private final ConcurrentMap<Long,RelationshipType> postIsLocatedInRelationshipTypeCache =
            Maps.newConcurrentMap();
    private final ConcurrentMap<Long,RelationshipType> commentIsLocatedInRelationshipTypeCache =
            Maps.newConcurrentMap();
    private final ConcurrentMap<Long,RelationshipType> hasMemberRelationshipTypeCache =
            Maps.newConcurrentMap();

    private final AtomicReference<ResizeResult> postHasCreatorResizeResultRef;
    private final AtomicReference<ResizeResult> commentHasCreatorResizeResultRef;
    private final AtomicReference<ResizeResult> worksAtResizeResultRef;
    private final AtomicReference<ResizeResult> postIsLocatedInResizeResultRef;
    private final AtomicReference<ResizeResult> commentIsLocatedInResizeResultRef;
    private final AtomicReference<ResizeResult> hasMemberResizeResultRef;

    public TimeStampedRelationshipTypesCache()
    {
        this.postHasCreatorResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Long.MAX_VALUE,
                                Long.MIN_VALUE ) );

        this.commentHasCreatorResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Long.MAX_VALUE,
                                Long.MIN_VALUE ) );

        this.worksAtResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Integer.MAX_VALUE,
                                Integer.MIN_VALUE ) );

        this.postIsLocatedInResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Long.MAX_VALUE,
                                Long.MIN_VALUE ) );

        this.commentIsLocatedInResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Long.MAX_VALUE,
                                Long.MIN_VALUE ) );

        this.hasMemberResizeResultRef =
                new AtomicReference<>(
                        new ResizeResult(
                                new RelationshipType[0],
                                new long[0],
                                Long.MAX_VALUE,
                                Long.MIN_VALUE ) );
    }

    public static RelationshipType[] joinArrays( RelationshipType[] array1, RelationshipType[] array2 )
    {
        RelationshipType[] hasCreatorRelationshipTypes = new RelationshipType[array1.length + array2.length];
        System.arraycopy(
                array1,
                0,
                hasCreatorRelationshipTypes,
                0,
                array1.length );
        System.arraycopy(
                array2,
                0,
                hasCreatorRelationshipTypes,
                array1.length,
                array2.length );
        return hasCreatorRelationshipTypes;
    }

    // ========================================================================================
    //                                POST HAS CREATOR
    // ========================================================================================

    public RelationshipType[] postHasCreatorForDateRange(
            final Calendar calendar,
            final long minDateAtResolution,
            final long maxDateAtResolution,
            final QueryDateUtil dateUtil ) throws DbException
    {
        if ( minDateAtResolution > maxDateAtResolution )
        {
            throw new DbException( "Minimum date must not be greater than maximum date" );
        }
        ResizeResult hasCreatorResizeResult = postHasCreatorResizeResultRef.get();
        if ( minDateAtResolution < hasCreatorResizeResult.minEncodedDateAtResolution() ||
             maxDateAtResolution > hasCreatorResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizePostHasCreatorArrays(
                    calendar,
                    minDateAtResolution,
                    maxDateAtResolution,
                    dateUtil
            );
            hasCreatorResizeResult = postHasCreatorResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                maxDateAtResolution,
                hasCreatorResizeResult.timeStampedRelationshipTypes(),
                hasCreatorResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] postHasCreatorForAllDates()
    {
        return postHasCreatorResizeResultRef.get().timeStampedRelationshipTypes();
    }

    public boolean resizePostHasCreatorForNewDate(
            final long encodedDateAtResolution,
            final Calendar calendar,
            final QueryDateUtil dateUtil )
    {
        ResizeResult postHasCreatorResizeResult = postHasCreatorResizeResultRef.get();
        if ( encodedDateAtResolution < postHasCreatorResizeResult.minEncodedDateAtResolution() ||
             encodedDateAtResolution > postHasCreatorResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizePostHasCreatorArrays(
                    calendar,
                    Math.min( encodedDateAtResolution, postHasCreatorResizeResult.minEncodedDateAtResolution() ),
                    Math.max( encodedDateAtResolution, postHasCreatorResizeResult.maxEncodedDateAtResolution() ),
                    dateUtil
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType postHasCreatorForDateAtResolution(
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return relationshipTypeForDate(
                encodedDateAtResolution,
                Rels.POST_HAS_CREATOR,
                postHasCreatorRelationshipTypeCache,
                dateUtil
        );
    }

    // ========================================================================================
    //                                COMMENT HAS CREATOR
    // ========================================================================================

    public RelationshipType[] commentHasCreatorForDateRange(
            final Calendar calendar,
            final long minDateAtResolution,
            final long maxDateAtResolution,
            final QueryDateUtil dateUtil ) throws DbException
    {
        if ( minDateAtResolution > maxDateAtResolution )
        {
            throw new DbException( "Minimum date must not be greater than maximum date" );
        }
        ResizeResult commentHasCreatorResizeResult = commentHasCreatorResizeResultRef.get();
        if ( minDateAtResolution < commentHasCreatorResizeResult.minEncodedDateAtResolution() ||
             maxDateAtResolution > commentHasCreatorResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeCommentHasCreatorArrays(
                    calendar,
                    minDateAtResolution,
                    maxDateAtResolution,
                    dateUtil
            );
            commentHasCreatorResizeResult = commentHasCreatorResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                maxDateAtResolution,
                commentHasCreatorResizeResult.timeStampedRelationshipTypes(),
                commentHasCreatorResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] commentHasCreatorForAllDates()
    {
        return commentHasCreatorResizeResultRef.get().timeStampedRelationshipTypes();
    }

    public boolean resizeCommentHasCreatorForNewDate(
            final long encodedDateAtResolution,
            final Calendar calendar,
            final QueryDateUtil dateUtil )
    {
        ResizeResult commentHasCreatorResizeResult = commentHasCreatorResizeResultRef.get();
        if ( encodedDateAtResolution < commentHasCreatorResizeResult.minEncodedDateAtResolution() ||
             encodedDateAtResolution > commentHasCreatorResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeCommentHasCreatorArrays(
                    calendar,
                    Math.min( encodedDateAtResolution, commentHasCreatorResizeResult.minEncodedDateAtResolution() ),
                    Math.max( encodedDateAtResolution, commentHasCreatorResizeResult.maxEncodedDateAtResolution() ),
                    dateUtil
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType commentHasCreatorForDateAtResolution(
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return relationshipTypeForDate(
                encodedDateAtResolution,
                Rels.COMMENT_HAS_CREATOR,
                commentHasCreatorRelationshipTypeCache,
                dateUtil
        );
    }

    // ========================================================================================
    //                                WORKS AT
    // ========================================================================================

    public RelationshipType[] worksAtForYearsBefore( final int maxYear ) throws DbException
    {
        ResizeResult worksAtResizeResult = worksAtResizeResultRef.get();
        if ( 0 == worksAtResizeResult.encodedDatesAtResolution().length )
        {
            throw new DbException( "Works at years has not been initialized" );
        }
        if ( maxYear < (int) worksAtResizeResult.minEncodedDateAtResolution() )
        {
            throw new DbException( format( "Max year (%s) must not be lower than min year (%s)",
                                           maxYear,
                                           worksAtResizeResult.minEncodedDateAtResolution() ) );
        }
        int minYear = (int) worksAtResizeResult.minEncodedDateAtResolution();
        return worksAtForYearsBetween( minYear, maxYear );
    }

    private RelationshipType[] worksAtForYearsBetween(
            final int minYear,
            final int maxYear )
    {
        ResizeResult worksAtResizeResult = worksAtResizeResultRef.get();
        if ( minYear < worksAtResizeResult.minEncodedDateAtResolution() ||
             maxYear > worksAtResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeWorksAtArrays(
                    minYear,
                    maxYear
            );
            worksAtResizeResult = worksAtResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minYear,
                maxYear,
                worksAtResizeResult.timeStampedRelationshipTypes(),
                worksAtResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] worksAtForAllYears() throws DbException
    {
        ResizeResult resizeResult = worksAtResizeResultRef.get();
        if ( 0 == resizeResult.encodedDatesAtResolution().length )
        {
            throw new DbException( "Works at years has not been initialized" );
        }
        return resizeResult.timeStampedRelationshipTypes();
    }

    public boolean resizeWorksAtForNewYear( final int year )
    {
        ResizeResult resizeResult = worksAtResizeResultRef.get();

        if ( year < resizeResult.minEncodedDateAtResolution() )
        {
            int minYear = year;
            int maxYear = (Integer.MIN_VALUE == resizeResult.maxEncodedDateAtResolution())
                          ? year
                          : (int) resizeResult.maxEncodedDateAtResolution();
            SYNCHRONIZED_resizeWorksAtArrays(
                    minYear,
                    maxYear
            );
            return true;
        }
        else if ( year > resizeResult.maxEncodedDateAtResolution() )
        {
            int minYear = (Integer.MAX_VALUE == resizeResult.minEncodedDateAtResolution())
                          ? year
                          : (int) resizeResult.minEncodedDateAtResolution();
            int maxYear = year;
            SYNCHRONIZED_resizeWorksAtArrays(
                    minYear,
                    maxYear
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType worksAtForYear( final int year )
    {
        RelationshipType relationshipType = worksAtRelationshipTypeCache.get( year );
        if ( null == relationshipType )
        {
            relationshipType = RelationshipType.withName(
                    Rels.WORKS_AT.name() +
                    WORKS_AT_YEAR_STRING_FORMAT.format( year ) );
            worksAtRelationshipTypeCache.put( year, relationshipType );
        }
        return relationshipType;
    }

    // ========================================================================================
    //                                POST IS LOCATED IN
    // ========================================================================================

    public RelationshipType[] postIsLocatedInForDateRange(
            final Calendar calendar,
            final long minDateAtResolution,
            final long maxDateAtResolution,
            final QueryDateUtil dateUtil ) throws DbException
    {
        if ( minDateAtResolution > maxDateAtResolution )
        {
            throw new DbException( "Minimum date must not be greater than maximum date" );
        }
        ResizeResult postIsLocatedInResizeResult = postIsLocatedInResizeResultRef.get();
        if ( minDateAtResolution < postIsLocatedInResizeResult.minEncodedDateAtResolution() ||
             maxDateAtResolution > postIsLocatedInResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizePostIsLocatedInArrays(
                    calendar,
                    minDateAtResolution,
                    maxDateAtResolution,
                    dateUtil
            );
            postIsLocatedInResizeResult = postIsLocatedInResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                maxDateAtResolution,
                postIsLocatedInResizeResult.timeStampedRelationshipTypes(),
                postIsLocatedInResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] postIsLocatedInForAllDates()
    {
        return postIsLocatedInResizeResultRef.get().timeStampedRelationshipTypes();
    }

    public boolean resizePostIsLocatedInForNewDate(
            final long encodedDateAtResolution,
            final Calendar calendar,
            final QueryDateUtil dateUtil )
    {
        ResizeResult postIsLocatedInResizeResult = postIsLocatedInResizeResultRef.get();
        if ( encodedDateAtResolution < postIsLocatedInResizeResult.minEncodedDateAtResolution() ||
             encodedDateAtResolution > postIsLocatedInResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizePostIsLocatedInArrays(
                    calendar,
                    Math.min( encodedDateAtResolution, postIsLocatedInResizeResult.minEncodedDateAtResolution() ),
                    Math.max( encodedDateAtResolution, postIsLocatedInResizeResult.maxEncodedDateAtResolution() ),
                    dateUtil
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType postIsLocatedInForDateAtResolution(
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return relationshipTypeForDate(
                encodedDateAtResolution,
                Rels.POST_IS_LOCATED_IN,
                postIsLocatedInRelationshipTypeCache,
                dateUtil
        );
    }

    // ========================================================================================
    //                                COMMENT IS LOCATED IN
    // ========================================================================================

    public RelationshipType[] commentIsLocatedInForDateRange(
            final Calendar calendar,
            final long minDateAtResolution,
            final long maxDateAtResolution,
            final QueryDateUtil dateUtil ) throws DbException
    {
        if ( minDateAtResolution > maxDateAtResolution )
        {
            throw new DbException( "Minimum date must not be greater than maximum date" );
        }
        ResizeResult commentIsLocatedInResizeResult = commentIsLocatedInResizeResultRef.get();
        if ( minDateAtResolution < commentIsLocatedInResizeResult.minEncodedDateAtResolution() ||
             maxDateAtResolution > commentIsLocatedInResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeCommentIsLocatedInArrays(
                    calendar,
                    minDateAtResolution,
                    maxDateAtResolution,
                    dateUtil
            );
            commentIsLocatedInResizeResult = commentIsLocatedInResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                maxDateAtResolution,
                commentIsLocatedInResizeResult.timeStampedRelationshipTypes(),
                commentIsLocatedInResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] commentIsLocatedInForAllDates()
    {
        return commentIsLocatedInResizeResultRef.get().timeStampedRelationshipTypes();
    }

    public boolean resizeCommentIsLocatedInForNewDate(
            final long encodedDateAtResolution,
            final Calendar calendar,
            final QueryDateUtil dateUtil )
    {
        ResizeResult commentIsLocatedInResizeResult = commentIsLocatedInResizeResultRef.get();
        if ( encodedDateAtResolution < commentIsLocatedInResizeResult.minEncodedDateAtResolution() ||
             encodedDateAtResolution > commentIsLocatedInResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeCommentIsLocatedInArrays(
                    calendar,
                    Math.min( encodedDateAtResolution, commentIsLocatedInResizeResult.minEncodedDateAtResolution() ),
                    Math.max( encodedDateAtResolution, commentIsLocatedInResizeResult.maxEncodedDateAtResolution() ),
                    dateUtil
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType commentIsLocatedInForDateAtResolution(
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return relationshipTypeForDate(
                encodedDateAtResolution,
                Rels.COMMENT_IS_LOCATED_IN,
                commentIsLocatedInRelationshipTypeCache,
                dateUtil
        );
    }

    // ========================================================================================
    //                                HAS MEMBER
    // ========================================================================================

    public RelationshipType[] hasMemberForDatesAfter( final long minDateAtResolution ) throws DbException
    {
        ResizeResult hasMemberResizeResult = hasMemberResizeResultRef.get();
        if ( minDateAtResolution < hasMemberResizeResult.minEncodedDateAtResolution() )
        {
            throw new DbException(
                    format( "Date (%s) must not be lower than minimum recorded date (%s)\n%s",
                            minDateAtResolution,
                            hasMemberResizeResult.minEncodedDateAtResolution(),
                            Arrays.toString( hasMemberResizeResult.encodedDatesAtResolution() ) ) );
        }
        if ( minDateAtResolution > hasMemberResizeResult.maxEncodedDateAtResolution() )
        {
            // TODO uncomment when param gen fixed -- currently creates params out of range sometimes
//            throw new DbException(
//                    format( "Date (%s) must not be greater than maximum recorded date (%s)\n%s",
//                            minDateAtResolution,
//                            hasMemberResizeResult.maxEncodedDateAtResolution(),
//                            Arrays.toString( hasMemberResizeResult.encodedDatesAtResolution() ) ) );
            // TODO remove when param gen fixed
            return new RelationshipType[0];
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                hasMemberResizeResult.maxEncodedDateAtResolution(),
                hasMemberResizeResult.timeStampedRelationshipTypes(),
                hasMemberResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] hasMemberForDateRange(
            final Calendar calendar,
            final long minDateAtResolution,
            final long maxDateAtResolution,
            final QueryDateUtil dateUtil ) throws DbException
    {
        if ( minDateAtResolution > maxDateAtResolution )
        {
            throw new DbException( "Minimum date must not be greater than maximum date" );
        }
        ResizeResult hasMemberResizeResult = hasMemberResizeResultRef.get();
        if ( minDateAtResolution < hasMemberResizeResult.minEncodedDateAtResolution() ||
             maxDateAtResolution > hasMemberResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeHasMemberArrays(
                    calendar,
                    minDateAtResolution,
                    maxDateAtResolution,
                    dateUtil
            );
            hasMemberResizeResult = hasMemberResizeResultRef.get();
        }
        return relationshipTypesForDateRange(
                minDateAtResolution,
                maxDateAtResolution,
                hasMemberResizeResult.timeStampedRelationshipTypes(),
                hasMemberResizeResult.encodedDatesAtResolution()
        );
    }

    public RelationshipType[] hasMemberForAllDates()
    {
        return hasMemberResizeResultRef.get().timeStampedRelationshipTypes();
    }

    public boolean resizeHasMemberForNewDate(
            final long encodedDateAtResolution,
            final Calendar calendar,
            final QueryDateUtil dateUtil )
    {
        ResizeResult hasMemberResizeResult = hasMemberResizeResultRef.get();
        if ( encodedDateAtResolution < hasMemberResizeResult.minEncodedDateAtResolution() ||
             encodedDateAtResolution > hasMemberResizeResult.maxEncodedDateAtResolution() )
        {
            SYNCHRONIZED_resizeHasMemberArrays(
                    calendar,
                    Math.min( encodedDateAtResolution, hasMemberResizeResult.minEncodedDateAtResolution() ),
                    Math.max( encodedDateAtResolution, hasMemberResizeResult.maxEncodedDateAtResolution() ),
                    dateUtil
            );
            return true;
        }
        else
        {
            return false;
        }
    }

    public RelationshipType hasMemberForDateAtResolution(
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return relationshipTypeForDate(
                encodedDateAtResolution,
                Rels.HAS_MEMBER,
                hasMemberRelationshipTypeCache,
                dateUtil
        );
    }

    // ========================================================================================
    // ========================================================================================
    //                                PRIVATE METHODS
    // ========================================================================================
    // ========================================================================================

    private synchronized void SYNCHRONIZED_resizePostHasCreatorArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        ResizeResult newResizeResult = resizeArrays(
                calendar,
                encodedMinDateAtResolution,
                encodedMaxDateAtResolution,
                postHasCreatorResizeResultRef.get(),
                postHasCreatorRelationshipTypeCache,
                Rels.POST_HAS_CREATOR,
                dateUtil
        );
        postHasCreatorResizeResultRef.set( newResizeResult );
    }

    private synchronized void SYNCHRONIZED_resizeCommentHasCreatorArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        ResizeResult newResizeResult = resizeArrays(
                calendar,
                encodedMinDateAtResolution,
                encodedMaxDateAtResolution,
                commentHasCreatorResizeResultRef.get(),
                commentHasCreatorRelationshipTypeCache,
                Rels.COMMENT_HAS_CREATOR,
                dateUtil
        );
        commentHasCreatorResizeResultRef.set( newResizeResult );
    }

    private synchronized void SYNCHRONIZED_resizePostIsLocatedInArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        ResizeResult newResizeResult = resizeArrays(
                calendar,
                encodedMinDateAtResolution,
                encodedMaxDateAtResolution,
                postIsLocatedInResizeResultRef.get(),
                postIsLocatedInRelationshipTypeCache,
                Rels.POST_IS_LOCATED_IN,
                dateUtil
        );
        postIsLocatedInResizeResultRef.set( newResizeResult );
    }

    private synchronized void SYNCHRONIZED_resizeCommentIsLocatedInArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        ResizeResult newResizeResult = resizeArrays(
                calendar,
                encodedMinDateAtResolution,
                encodedMaxDateAtResolution,
                commentIsLocatedInResizeResultRef.get(),
                commentIsLocatedInRelationshipTypeCache,
                Rels.COMMENT_IS_LOCATED_IN,
                dateUtil
        );
        commentIsLocatedInResizeResultRef.set( newResizeResult );
    }

    private synchronized void SYNCHRONIZED_resizeHasMemberArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        ResizeResult newResizeResult = resizeArrays(
                calendar,
                encodedMinDateAtResolution,
                encodedMaxDateAtResolution,
                hasMemberResizeResultRef.get(),
                hasMemberRelationshipTypeCache,
                Rels.HAS_MEMBER,
                dateUtil
        );
        hasMemberResizeResultRef.set( newResizeResult );
    }

    private synchronized void SYNCHRONIZED_resizeWorksAtArrays(
            final int minYear,
            final int maxYear )
    {
        ResizeResult resizeResult = worksAtResizeResultRef.get();
        // Initialize array
        if ( 0 == resizeResult.encodedDatesAtResolution().length )
        {
            int year = minYear;
            int requiredArraySize = 1;
            while ( year < maxYear )
            {
                year++;
                requiredArraySize++;
            }
            long[] worksAtTimeStamps = new long[requiredArraySize];
            RelationshipType[] newWorksAtTimeStampedRelationshipTypes = new RelationshipType[requiredArraySize];
            year = minYear;
            for ( int i = 0; i < requiredArraySize; i++ )
            {
                worksAtTimeStamps[i] = year;
                newWorksAtTimeStampedRelationshipTypes[i] = worksAtForYear( year );
                year++;
            }
            worksAtResizeResultRef.set(
                    new ResizeResult(
                            newWorksAtTimeStampedRelationshipTypes,
                            worksAtTimeStamps,
                            worksAtTimeStamps[0],
                            worksAtTimeStamps[worksAtTimeStamps.length - 1]
                    )
            );
        }
        else
        {
            // calculate amount to resize low end of arrays
            int elementsToAddToLowEnd = 0;
            int year;
            if ( minYear < resizeResult.minEncodedDateAtResolution() )
            {
                year = minYear;
                while ( year < resizeResult.minEncodedDateAtResolution() )
                {
                    year++;
                    elementsToAddToLowEnd++;
                }
            }

            // calculate amount to resize high end of arrays
            int elementsToAddToHighEnd = 0;
            int firstHighEndYear = Integer.MIN_VALUE;
            if ( maxYear > resizeResult.maxEncodedDateAtResolution() )
            {
                year = maxYear;
                while ( year > resizeResult.maxEncodedDateAtResolution() )
                {
                    firstHighEndYear = year;
                    elementsToAddToHighEnd++;
                    year--;
                }
            }

            // actually resize arrays
            final int newArraySize =
                    elementsToAddToLowEnd + resizeResult.encodedDatesAtResolution().length + elementsToAddToHighEnd;
            final long[] newWorksAtTimeStamps = new long[newArraySize];
            final RelationshipType[] newWorksAtTimeStampedRelationshipTypes = new RelationshipType[newArraySize];

            // add new elements to low end
            if ( elementsToAddToLowEnd > 0 )
            {
                year = minYear;
                for ( int i = 0; i < elementsToAddToLowEnd; i++ )
                {
                    newWorksAtTimeStamps[i] = year;
                    newWorksAtTimeStampedRelationshipTypes[i] = worksAtForYear( year );
                    year++;
                }
            }

            // add old elements
            System.arraycopy(
                    resizeResult.encodedDatesAtResolution(), 0,
                    newWorksAtTimeStamps, elementsToAddToLowEnd,
                    resizeResult.encodedDatesAtResolution().length
            );
            System.arraycopy(
                    resizeResult.timeStampedRelationshipTypes(), 0,
                    newWorksAtTimeStampedRelationshipTypes, elementsToAddToLowEnd,
                    resizeResult.timeStampedRelationshipTypes().length
            );

            // add new elements to high end
            if ( elementsToAddToHighEnd > 0 )
            {
                year = firstHighEndYear;
                for ( int i = elementsToAddToLowEnd + resizeResult.encodedDatesAtResolution().length; i < newArraySize;
                        i++ )
                {
                    newWorksAtTimeStamps[i] = year;
                    newWorksAtTimeStampedRelationshipTypes[i] = worksAtForYear( year );
                    year++;
                }
            }

            worksAtResizeResultRef.set(
                    new ResizeResult(
                            newWorksAtTimeStampedRelationshipTypes,
                            newWorksAtTimeStamps,
                            newWorksAtTimeStamps[0],
                            newWorksAtTimeStamps[newWorksAtTimeStamps.length - 1]
                    )
            );
        }
    }

    private ResizeResult resizeArrays(
            final Calendar calendar,
            final long encodedMinDateAtResolution,
            final long encodedMaxDateAtResolution,
            final ResizeResult resizeResult,
            final Map<Long,RelationshipType> relationshipTypeCache,
            final RelationshipType baseRelationshipType,
            final QueryDateUtil dateUtil )
    {
        // Initialize arrays
        if ( 0 == resizeResult.encodedDatesAtResolution().length )
        {
            final long[] newTimeStamps = dateUtil.dateCodec().encodedDatesAtResolutionForRange(
                    encodedMinDateAtResolution,
                    encodedMaxDateAtResolution,
                    calendar );
            final RelationshipType[] newTimeStampedRelationshipTypes = new RelationshipType[newTimeStamps.length];
            for ( int i = 0; i < newTimeStamps.length; i++ )
            {
                newTimeStampedRelationshipTypes[i] = relationshipTypeForDate(
                        newTimeStamps[i],
                        baseRelationshipType,
                        relationshipTypeCache,
                        dateUtil );
            }
            return new ResizeResult(
                    newTimeStampedRelationshipTypes,
                    newTimeStamps,
                    newTimeStamps[0],
                    newTimeStamps[newTimeStamps.length - 1]
            );
        }
        else
        {
            // calculate amount to resize low end of arrays
            int elementsToAddToLowEnd = 0;
            if ( encodedMinDateAtResolution < resizeResult.minEncodedDateAtResolution() )
            {
                dateUtil.dateCodec().populateCalendarFromEncodedDateAtResolution(
                        encodedMinDateAtResolution,
                        calendar );
                while ( dateUtil.dateCodec().calendarToEncodedDateAtResolution( calendar ) <
                        resizeResult.minEncodedDateAtResolution() )
                {
                    dateUtil.dateCodec().incrementCalendarByTimestampResolution( calendar, 1 );
                    elementsToAddToLowEnd++;
                }
            }

            // calculate amount to resize high end of arrays
            int elementsToAddToHighEnd = 0;
            long firstHighEndDateAtResolution = Long.MIN_VALUE;
            if ( encodedMaxDateAtResolution > resizeResult.maxEncodedDateAtResolution() )
            {
                dateUtil.dateCodec().populateCalendarFromEncodedDateAtResolution(
                        encodedMaxDateAtResolution,
                        calendar );
                long nextDateAtResolution = encodedMaxDateAtResolution;
                while ( nextDateAtResolution > resizeResult.maxEncodedDateAtResolution() )
                {
                    firstHighEndDateAtResolution = nextDateAtResolution;
                    elementsToAddToHighEnd++;
                    dateUtil.dateCodec().incrementCalendarByTimestampResolution( calendar, -1 );
                    nextDateAtResolution = dateUtil.dateCodec().calendarToEncodedDateAtResolution( calendar );
                }
            }

            // actually resize arrays
            final int newArraySize = elementsToAddToLowEnd +
                                     resizeResult.encodedDatesAtResolution().length +
                                     elementsToAddToHighEnd;
            final long[] newHasCreatorTimeStamps = new long[newArraySize];
            final RelationshipType[] newTimeStampedRelationshipTypes = new RelationshipType[newArraySize];

            // add new elements to low end
            if ( elementsToAddToLowEnd > 0 )
            {
                dateUtil.dateCodec().populateCalendarFromEncodedDateAtResolution(
                        encodedMinDateAtResolution,
                        calendar );
                for ( int i = 0; i < elementsToAddToLowEnd; i++ )
                {
                    long encodedDateAtResolution = dateUtil.dateCodec().calendarToEncodedDateAtResolution( calendar );
                    newHasCreatorTimeStamps[i] = encodedDateAtResolution;
                    newTimeStampedRelationshipTypes[i] = relationshipTypeForDate(
                            encodedDateAtResolution,
                            baseRelationshipType,
                            relationshipTypeCache,
                            dateUtil
                    );
                    dateUtil.dateCodec().incrementCalendarByTimestampResolution( calendar, 1 );
                }
            }

            // add old elements
            System.arraycopy(
                    resizeResult.encodedDatesAtResolution(), 0,
                    newHasCreatorTimeStamps, elementsToAddToLowEnd,
                    resizeResult.encodedDatesAtResolution().length
            );
            System.arraycopy(
                    resizeResult.timeStampedRelationshipTypes(), 0,
                    newTimeStampedRelationshipTypes, elementsToAddToLowEnd,
                    resizeResult.timeStampedRelationshipTypes().length
            );

            // add new elements to high end
            if ( elementsToAddToHighEnd > 0 )
            {
                dateUtil.dateCodec().populateCalendarFromEncodedDateAtResolution(
                        firstHighEndDateAtResolution,
                        calendar );
                for ( int i = elementsToAddToLowEnd + resizeResult.encodedDatesAtResolution().length;
                      i < newArraySize; i++ )
                {
                    long encodedDateAtResolution = dateUtil.dateCodec().calendarToEncodedDateAtResolution( calendar );
                    newHasCreatorTimeStamps[i] = encodedDateAtResolution;
                    newTimeStampedRelationshipTypes[i] = relationshipTypeForDate(
                            encodedDateAtResolution,
                            baseRelationshipType,
                            relationshipTypeCache,
                            dateUtil
                    );
                    dateUtil.dateCodec().incrementCalendarByTimestampResolution( calendar, 1 );
                }
            }
            return new ResizeResult(
                    newTimeStampedRelationshipTypes,
                    newHasCreatorTimeStamps,
                    newHasCreatorTimeStamps[0],
                    newHasCreatorTimeStamps[newHasCreatorTimeStamps.length - 1]
            );
        }
    }

    private RelationshipType[] relationshipTypesForDateRange(
            final long minEncodedDateAtResolution,
            final long maxEncodedDateAtResolution,
            final RelationshipType[] timeStampedRelationshipTypes,
            final long[] encodedDatesAtResolution )
    {
        final int originalLength = timeStampedRelationshipTypes.length;

        int i = 0;
        while ( minEncodedDateAtResolution != encodedDatesAtResolution[i] )
        {
            i++;
        }
        final int rangeStartIndex = i;

        for ( ; i < originalLength; i++ )
        {
            if ( maxEncodedDateAtResolution == encodedDatesAtResolution[i] )
            {
                break;
            }
        }
        final int rangeEndIndex = i;

        if ( rangeStartIndex == 0 && rangeEndIndex == originalLength - 1 )
        {
            return timeStampedRelationshipTypes;
        }
        else
        {
            return Arrays.copyOfRange( timeStampedRelationshipTypes, rangeStartIndex, rangeEndIndex + 1 );
        }
    }

    private RelationshipType relationshipTypeForDate(
            final long encodedDateAtResolution,
            final RelationshipType baseRelationshipType,
            final Map<Long,RelationshipType> relationshipTypeCache,
            final QueryDateUtil dateUtil )
    {
        RelationshipType relationshipType = relationshipTypeCache.get( encodedDateAtResolution );
        if ( null == relationshipType )
        {
            relationshipType = timeStampedRelationshipType(
                    baseRelationshipType,
                    encodedDateAtResolution,
                    dateUtil );
            relationshipTypeCache.put( encodedDateAtResolution, relationshipType );
        }
        return relationshipType;
    }

    private RelationshipType timeStampedRelationshipType(
            final RelationshipType relationshipType,
            final long encodedDateAtResolution,
            final QueryDateUtil dateUtil )
    {
        return RelationshipType.withName(
                relationshipType.name() +
                dateUtil.dateCodec().encodedDateAtResolutionToString( encodedDateAtResolution )
        );
    }

    private static class ResizeResult
    {
        private final RelationshipType[] timeStampedRelationshipTypes;
        private final long[] encodedDatesAtResolution;
        private final long minEncodedDateAtResolution;
        private final long maxEncodedDateAtResolution;

        private ResizeResult(
                RelationshipType[] timeStampedRelationshipTypes,
                long[] encodedDatesAtResolution,
                long minEncodedDateAtResolution,
                long maxEncodedDateAtResolution )
        {
            this.timeStampedRelationshipTypes = timeStampedRelationshipTypes;
            this.encodedDatesAtResolution = encodedDatesAtResolution;
            this.minEncodedDateAtResolution = minEncodedDateAtResolution;
            this.maxEncodedDateAtResolution = maxEncodedDateAtResolution;
        }

        public RelationshipType[] timeStampedRelationshipTypes()
        {
            return timeStampedRelationshipTypes;
        }

        public long[] encodedDatesAtResolution()
        {
            return encodedDatesAtResolution;
        }

        public long minEncodedDateAtResolution()
        {
            return minEncodedDateAtResolution;
        }

        public long maxEncodedDateAtResolution()
        {
            return maxEncodedDateAtResolution;
        }

        @Override
        public String toString()
        {
            return "ResizeResult{\n" +
                   "  timeStampedRelationshipTypes=" + Arrays.toString( timeStampedRelationshipTypes ) + "\n" +
                   "  encodedDatesAtResolution=" + Arrays.toString( encodedDatesAtResolution ) + "\n" +
                   "  minEncodedDateAtResolution=" + minEncodedDateAtResolution + "\n" +
                   "  maxEncodedDateAtResolution=" + maxEncodedDateAtResolution + "\n" +
                   "}";
        }
    }
}
