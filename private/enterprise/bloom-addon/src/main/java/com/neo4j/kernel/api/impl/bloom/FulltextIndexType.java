/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

/**
 * Fulltext index type.
 */
public enum FulltextIndexType
{
    NODES
            {
                @Override
                public String toString()
                {
                    return "Nodes";
                }
            },
    RELATIONSHIPS
            {
                @Override
                public String toString()
                {
                    return "Relationships";
                }
            }
}
