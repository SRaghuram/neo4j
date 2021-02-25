/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

/**
 * Collects all the state that must be recovered after a restart.
 */
class State
{
    private final Segments segments;
    private final Terms terms;

    private long prevIndex;
    private long prevTerm;
    private long appendIndex;

    State( Segments segments, Terms terms )
    {
        this( segments, terms, -1, -1, -1 );
    }

    State( Segments segments, Terms terms, long appendIndex, long prevIndex, long prevTerm )
    {
        this.segments = segments;
        this.terms = terms;
        this.appendIndex = appendIndex;
        this.prevIndex = prevIndex;
        this.prevTerm = prevTerm;
    }

    void setPrevIndex( long prevIndex )
    {
        this.prevIndex = prevIndex;
    }

    void setPrevTerm( long prevTerm )
    {
        this.prevTerm = prevTerm;
    }

    void setAppendIndex( long appendIndex )
    {
        this.appendIndex = appendIndex;
    }

    long prevIndex()
    {
        return prevIndex;
    }

    long prevTerm()
    {
        return prevTerm;
    }

    long appendIndex()
    {
        return appendIndex;
    }

    Segments segments()
    {
        return segments;
    }

    Terms terms()
    {
        return terms;
    }

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
