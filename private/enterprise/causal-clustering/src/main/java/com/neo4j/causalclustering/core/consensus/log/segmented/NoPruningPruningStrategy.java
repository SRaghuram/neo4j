/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

class NoPruningPruningStrategy implements CoreLogPruningStrategy
{
    @Override
    public long getIndexToKeep( Segments segments )
    {
        return -1;
    }
}
