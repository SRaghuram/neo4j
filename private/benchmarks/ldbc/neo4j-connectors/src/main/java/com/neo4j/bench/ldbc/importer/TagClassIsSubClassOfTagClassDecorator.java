/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

public class TagClassIsSubClassOfTagClassDecorator implements Decorator<InputRelationship>
{
    @Override
    public boolean isMutable()
    {
        return true;
    }

    @Override
    public InputRelationship apply( InputRelationship inputRelationship ) throws RuntimeException
    {
        // tags classes: id|name|url|isSubclassOf
        // NOTE: only pass through relationships that have both start and end nodes

        // id|id.val|name|name.val|url|url.val|isSubclassOf|isSubclassOf.val
        // 0 |1     |2   |3       |4  |5      |6           |7
        if ( null == inputRelationship.startNode() || null == inputRelationship.endNode() )
        {
            return new InputRelationship(
                    inputRelationship.sourceDescription(),
                    inputRelationship.lineNumber(),
                    inputRelationship.position(),
                    inputRelationship.properties(),
                    0L, // inputRelationship.firstPropertyId() causes NPE
                    -1,
                    -1,
                    inputRelationship.type(),
                    0 // inputRelationship.typeId() causes NPE
            );
        }
        else
        {
            return inputRelationship;
        }
    }
}
