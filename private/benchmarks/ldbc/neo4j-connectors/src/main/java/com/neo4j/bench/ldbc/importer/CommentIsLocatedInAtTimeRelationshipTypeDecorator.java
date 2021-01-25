/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.ImportDateUtil;
import com.neo4j.bench.ldbc.connection.LdbcDateCodecUtil;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;

import java.text.ParseException;
import java.util.Calendar;
import java.util.function.Supplier;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class CommentIsLocatedInAtTimeRelationshipTypeDecorator implements Decorator
{
    private final Supplier<ImportDateUtil> importDateUtilSupplier;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final GraphMetadataTracker metadataTracker;

    public CommentIsLocatedInAtTimeRelationshipTypeDecorator(
            Supplier<ImportDateUtil> importDateUtilSupplier,
            TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache,
            GraphMetadataTracker metadataTracker )
    {
        this.importDateUtilSupplier = importDateUtilSupplier;
        this.timeStampedRelationshipTypesCache = timeStampedRelationshipTypesCache;
        this.metadataTracker = metadataTracker;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        // comment is located in place - WITH TIME STAMP
        // comments: id|creationDate|locationIP|browserUsed|content|length|creator|place|replyOfPost|replyOfComment
        // NOTE: only creationDate is passed through
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
            private final ImportDateUtil importDateUtil = importDateUtilSupplier.get();

            @Override
            public boolean property( String key, Object value )
            {
                if ( "creationDate".equals( key ) )
                {
                    String creationDateString = (String) value;
                    long creationDateAtResolution;
                    try
                    {
                        long creationDate = importDateUtil.csvDateTimeToFormat( creationDateString, calendar );
                        creationDateAtResolution =
                                importDateUtil.queryDateUtil().formatToEncodedDateAtResolution( creationDate );
                        metadataTracker.recordCommentIsLocatedInDateAtResolution( creationDateAtResolution );
                    }
                    catch ( ParseException e )
                    {
                        throw new RuntimeException( String.format( "Invalid date string: %s", creationDateString ), e );
                    }

                    RelationshipType isLocatedInRelationshipType =
                            timeStampedRelationshipTypesCache.commentIsLocatedInForDateAtResolution(
                                    creationDateAtResolution,
                                    importDateUtil.queryDateUtil() );

                    return super.type( isLocatedInRelationshipType.name() );
                }
                else
                {
                    return true;
                }
            }
        };
    }
}
