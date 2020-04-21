/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling.v2.decoding;

public class RaftLogEntryTerms
{
    private final long[] term;

    RaftLogEntryTerms( long[] term )
    {
        this.term = term;
    }

    public long[] terms()
    {
        return term;
    }
}