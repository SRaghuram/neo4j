/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class PostHasCreatorAtTimeRelationshipTypeDecorator implements Decorator
{
    private final Supplier<ImportDateUtil> importDateUtilSupplier;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final GraphMetadataTracker metadataTracker;

    public PostHasCreatorAtTimeRelationshipTypeDecorator(
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
        // post has creator person - WITH TIME STAMP
        // posts: id|imageFile|creationDate|locationIP|browserUsed|language|content|length|creator|Forum.id|place|
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
                        metadataTracker.recordPostHasCreatorDateAtResolution( creationDateAtResolution );
                    }
                    catch ( ParseException e )
                    {
                        throw new RuntimeException( String.format( "Invalid date string: %s", creationDateString ), e );
                    }

                    RelationshipType hasCreatorRelationshipType =
                            timeStampedRelationshipTypesCache.postHasCreatorForDateAtResolution(
                                    creationDateAtResolution,
                                    importDateUtil.queryDateUtil() );

                    return super.property( key, value ) &&
                           super.type( hasCreatorRelationshipType.name() );
                }
                else
                {
                    return true;
                }
            }
        };
    }
}
