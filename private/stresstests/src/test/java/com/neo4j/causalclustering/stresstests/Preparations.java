/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

public enum Preparations
{
    IdReuseSetup
            {
                @Override
                Preparation create( Resources resources )
                {
                    return new IdReuse.IdReuseSetup( resources );
                }
            };

    abstract Preparation create( Resources resources );
}
