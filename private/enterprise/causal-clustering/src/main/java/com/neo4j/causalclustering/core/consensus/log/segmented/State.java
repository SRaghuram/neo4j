/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

/**
 * Collects all the state that must be recovered after a restart.
 */
public class State
{
    Segments segments;
    Terms terms;

    long prevIndex = -1;
    long prevTerm = -1;
    long appendIndex = -1;

    @Override
    public String toString()
    {
        return "State{" +
               "prevIndex=" + prevIndex +
               ", prevTerm=" + prevTerm +
               ", appendIndex=" + appendIndex +
               '}';
    }
}
