/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.replication.DistributedOperation;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.function.ThrowingBiConsumer;

class CommandBatcher
{
    private List<DistributedOperation> batch;
    private int maxBatchSize;
    private final ThrowingBiConsumer<Long,List<DistributedOperation>,Exception> applier;
    private long lastIndex;

    CommandBatcher( int maxBatchSize, ThrowingBiConsumer<Long,List<DistributedOperation>,Exception> applier )
    {
        this.batch = new ArrayList<>( maxBatchSize );
        this.maxBatchSize = maxBatchSize;
        this.applier = applier;
    }

    void add( long index, DistributedOperation operation ) throws Exception
    {
        assert batch.size() <= 0 || index == (lastIndex + 1);

        batch.add( operation );
        lastIndex = index;

        if ( batch.size() == maxBatchSize )
        {
            flush();
        }
    }

    void flush() throws Exception
    {
        applier.accept( lastIndex, batch );
        batch.clear();
    }
}
