/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;

import java.text.ParseException;
import java.util.Calendar;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class PostIsLocatedInAtTimeRelationshipTypeDecorator
        implements Decorator<InputRelationship>
{
    private final ThreadLocal<Calendar> calendarThreadLocal = new ThreadLocal<Calendar>()
    {
        @Override
        protected Calendar initialValue()
        {
            return LdbcDateCodec.newCalendar();
        }
    };
    private static final String[] EMPTY_STRING_ARRAY = new String[]{};
    private final ImportDateUtil importDateUtil;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final GraphMetadataTracker metadataTracker;

    public PostIsLocatedInAtTimeRelationshipTypeDecorator(
            ImportDateUtil importDateUtil,
            TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache,
            GraphMetadataTracker metadataTracker )
    {
        this.importDateUtil = importDateUtil;
        this.timeStampedRelationshipTypesCache = timeStampedRelationshipTypesCache;
        this.metadataTracker = metadataTracker;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputRelationship apply( InputRelationship inputRelationship ) throws RuntimeException
    {
        Calendar calendar = calendarThreadLocal.get();

        // post is located in place - WITH TIME STAMP
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place|
        // NOTE: only creationDate is passed through
        String creationDateString = (String) inputRelationship.properties()[1];
        long creationDate;
        long creationDateAtResolution;
        try
        {
            creationDate = importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
            creationDateAtResolution =
                    importDateUtil.queryDateUtil().formatToEncodedDateAtResolution( creationDate );
            metadataTracker.recordPostIsLocatedInDateAtResolution( creationDateAtResolution );
        }
        catch ( ParseException e )
        {
            throw new RuntimeException( String.format( "Invalid Date string: %s", creationDateString ), e );
        }
        RelationshipType hasCreatorRelationshipType =
                timeStampedRelationshipTypesCache.postIsLocatedInForDateAtResolution(
                        creationDateAtResolution,
                        importDateUtil.queryDateUtil()
                );
        String newType = hasCreatorRelationshipType.name();

        return new InputRelationship(
                inputRelationship.sourceDescription(),
                inputRelationship.lineNumber(),
                inputRelationship.position(),
                EMPTY_STRING_ARRAY,
                (inputRelationship.hasFirstPropertyId()) ? inputRelationship.firstPropertyId() : null,
                inputRelationship.startNodeGroup(),
                inputRelationship.startNode(),
                inputRelationship.endNodeGroup(),
                inputRelationship.endNode(),
                newType,
                inputRelationship.hasTypeId() ? inputRelationship.typeId() : null
        );
    }
}
