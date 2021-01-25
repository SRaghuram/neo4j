/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

public enum Validations
{
    ConsistencyCheck
            {
                @Override
                Validation create( Resources resources )
                {
                    return new ConsistencyCheck( resources );
                }
            },
    IdReuseUniqueFreeIds
            {
                @Override
                Validation create( Resources resources )
                {
                    return new IdReuse.UniqueFreeIds( resources );
                }
            };

    abstract Validation create( Resources resources );
}
