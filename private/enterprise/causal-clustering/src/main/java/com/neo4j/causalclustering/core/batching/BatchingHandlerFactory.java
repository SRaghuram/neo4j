/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.batching;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.identity.RaftGroupId;

import java.time.Instant;
import java.util.ArrayList;

class BatchingHandlerFactory
{
    private BatchingConfig batchConfig;
    private final ArrayList<RaftLogEntry> reusableEntryBatch;
    private final ArrayList<ReplicatedContent> reusableContentBatch;
    private BoundedPriorityQueue<RaftMessages.InboundRaftMessageContainer<?>> inQueue;

    BatchingHandlerFactory( BatchingConfig batchConfig,
            BoundedPriorityQueue<RaftMessages.InboundRaftMessageContainer<?>> inQueue )
    {
        this.batchConfig = batchConfig;
        this.reusableEntryBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.reusableContentBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.inQueue = inQueue;
    }

    /**
     * These are reusable as long as only one {@link BatchingHandler} is used at the time.
     */
    BatchingHandler batchingHandler( Instant receivedAt, RaftGroupId raftGroupId )
    {
        reusableContentBatch.clear();
        reusableEntryBatch.clear();
        return new BatchingHandler( receivedAt, raftGroupId, batchConfig, inQueue, reusableEntryBatch, reusableContentBatch );
    }
}
