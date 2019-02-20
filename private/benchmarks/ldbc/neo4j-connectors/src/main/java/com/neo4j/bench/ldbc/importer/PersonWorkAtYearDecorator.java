/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class PersonWorkAtYearDecorator implements Decorator<InputRelationship>
{
    private final GraphMetadataTracker graphMetaDataTracker;
    private final TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache;

    public PersonWorkAtYearDecorator(
            GraphMetadataTracker metadataTracker,
            TimeStampedRelationshipTypesCache timeStampedRelationshipTypesCache )
    {
        this.graphMetaDataTracker = metadataTracker;
        this.timeStampedRelationshipTypesCache = timeStampedRelationshipTypesCache;
    }

    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputRelationship apply( InputRelationship inputRelationship ) throws RuntimeException
    {
        // person works at organization
        // Person.id|Organisation.id|workFrom|
        // NOTE: only workFrom is passed through as property
        int workFromYear = (int) inputRelationship.properties()[1];
        graphMetaDataTracker.recordWorkFromYear( workFromYear );

        RelationshipType hasCreatorRelationshipType =
                timeStampedRelationshipTypesCache.worksAtForYear( workFromYear );
        String newType = hasCreatorRelationshipType.name();

        return new InputRelationship(
                inputRelationship.sourceDescription(),
                inputRelationship.lineNumber(),
                inputRelationship.position(),
                inputRelationship.properties(),
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
