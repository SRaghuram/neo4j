/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
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

public class PostHasCreatorAtTimeRelationshipTypeDecorator
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

    public PostHasCreatorAtTimeRelationshipTypeDecorator(
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

        // post has creator person - WITH TIME STAMP
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
            metadataTracker.recordPostHasCreatorDateAtResolution( creationDateAtResolution );
        }
        catch ( ParseException e )
        {
            throw new RuntimeException( String.format( "Invalid Date string: %s", creationDateString ), e );
        }
        RelationshipType hasCreatorRelationshipType =
                timeStampedRelationshipTypesCache.postHasCreatorForDateAtResolution(
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
