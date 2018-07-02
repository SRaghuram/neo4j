/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import java.util.function.Supplier;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterable;

/**
 * Fulltext index type.
 */
public enum FulltextIndexType
{
    NODES
            {
                @Override
                public Supplier<ResourceIterable<? extends Entity>> entityIterator( GraphDatabaseService db )
                {
                    return db::getAllNodes;
                }

                @Override
                public String toString()
                {
                    return "Nodes";
                }
            },
    RELATIONSHIPS
            {
                @Override
                public Supplier<ResourceIterable<? extends Entity>> entityIterator( GraphDatabaseService db )
                {
                    return db::getAllRelationships;
                }

                @Override
                public String toString()
                {
                    return "Relationships";
                }
            };

    public abstract Supplier<ResourceIterable<? extends Entity>> entityIterator( GraphDatabaseService db );
}
