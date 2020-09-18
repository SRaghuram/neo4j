/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.Replicator;

import java.util.function.Supplier;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.token.TokenRegistry;

public class ReplicatedPropertyKeyTokenHolder extends ReplicatedTokenHolder
{
    public ReplicatedPropertyKeyTokenHolder( NamedDatabaseId namedDatabaseId, TokenRegistry registry, Replicator replicator,
            IdGeneratorFactory idGeneratorFactory, Supplier<StorageEngine> storageEngineSupplier, PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker, LogEntryWriterFactory logEntryWriterFactory )
    {
        super( namedDatabaseId, registry, replicator, idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN, storageEngineSupplier, TokenType.PROPERTY,
                TransactionState::propertyKeyDoCreateForName, pageCacheTracer, memoryTracker, logEntryWriterFactory );
    }
}
