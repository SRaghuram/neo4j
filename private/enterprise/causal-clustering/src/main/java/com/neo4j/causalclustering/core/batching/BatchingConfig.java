/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

class BatchingConfig
{
    final int maxBatchCount;
    final long maxBatchBytes;

    BatchingConfig( int maxBatchCount, long maxBatchBytes )
    {
        this.maxBatchCount = maxBatchCount;
        this.maxBatchBytes = maxBatchBytes;
    }
}
