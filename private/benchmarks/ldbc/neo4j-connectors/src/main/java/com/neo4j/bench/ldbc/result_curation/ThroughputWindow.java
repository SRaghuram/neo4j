/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.result_curation;

class ThroughputWindow
{
    final long startTimeInclusiveAsMilli;
    final long endTimeExclusiveAsMilli;
    final long durationAsMilli;
    int count;

    ThroughputWindow( long startTimeInclusiveAsMilli, long endTimeExclusiveAsMilli )
    {
        this.startTimeInclusiveAsMilli = startTimeInclusiveAsMilli;
        this.endTimeExclusiveAsMilli = endTimeExclusiveAsMilli;
        this.durationAsMilli = endTimeExclusiveAsMilli - startTimeInclusiveAsMilli;
        this.count = 0;
    }
}
