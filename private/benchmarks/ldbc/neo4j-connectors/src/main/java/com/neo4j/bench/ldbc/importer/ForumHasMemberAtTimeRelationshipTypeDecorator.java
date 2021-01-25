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

public class ForumHasMemberAtTimeRelationshipTypeDecorator implements Decorator
{
    private final Supplier<ImportDateUtil> importDateUtilSupplier;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;
    private final GraphMetadataTracker metadataTracker;

    public ForumHasMemberAtTimeRelationshipTypeDecorator(
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
        // forum has member person - WITH TIME STAMP
        // forum has member person: : Forum.id|Person.id|joinDate
        // NOTE: only joinDate is passed through
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            private final Calendar calendar = LdbcDateCodecUtil.newCalendar();
            private final ImportDateUtil importDateUtil = importDateUtilSupplier.get();

            @Override
            public boolean property( String key, Object value )
            {
                if ( "joinDate".equals( key ) )
                {
                    String joinDateString = (String) value;
                    long joinDate;
                    long joinDateAtResolution;
                    try
                    {
                        joinDate = importDateUtil.csvDateTimeToFormat( joinDateString, calendar );
                        joinDateAtResolution =
                                importDateUtil.queryDateUtil().formatToEncodedDateAtResolution( joinDate );
                        metadataTracker.recordHasMemberDateAtResolution( joinDateAtResolution );
                    }
                    catch ( ParseException e )
                    {
                        throw new RuntimeException( String.format( "Invalid date string: %s", joinDateString ), e );
                    }

                    RelationshipType hasMemberRelationshipType =
                            timeStampedRelationshipTypesCache.hasMemberForDateAtResolution(
                                    joinDateAtResolution,
                                    importDateUtil.queryDateUtil() );

                    return super.property( key, joinDate ) &&
                           super.type( hasMemberRelationshipType.name() );
                }
                else
                {
                    return true;
                }
            }
        };
    }
}
