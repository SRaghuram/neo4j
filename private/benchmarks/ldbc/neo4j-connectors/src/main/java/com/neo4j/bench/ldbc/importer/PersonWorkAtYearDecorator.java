/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class PersonWorkAtYearDecorator implements Decorator
{
    private final GraphMetadataTracker metaDataTracker;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;

    public PersonWorkAtYearDecorator(
            GraphMetadataTracker metadataTracker,
            TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache )
    {
        this.metaDataTracker = metadataTracker;
        this.timeStampedRelationshipTypesCache = timeStampedRelationshipTypesCache;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        // person works at organization
        // Person.id|Organisation.id|workFrom|
        // NOTE: only workFrom is passed through as property
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            @Override
            public boolean property( String key, Object value )
            {
                if ( "workFrom".equals( key ) )
                {
                    int workFromYear = (int) value;
                    metaDataTracker.recordWorkFromYear( workFromYear );
                    RelationshipType workFromRelationshipType =
                            timeStampedRelationshipTypesCache.worksAtForYear( workFromYear );
                    return super.property( key, value ) &&
                           super.type( workFromRelationshipType.name() );
                }
                else
                {
                    return true;
                }
            }
        };
    }
}
