/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.Replicator;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.token.TokenRegistry;

import static org.neo4j.internal.id.IdType.RELATIONSHIP_TYPE_TOKEN;

public class ReplicatedRelationshipTypeTokenHolder extends ReplicatedTokenHolder
{
    public ReplicatedRelationshipTypeTokenHolder( NamedDatabaseId namedDatabaseId, TokenRegistry registry,
                                                  Replicator replicator, IdGeneratorFactory idGeneratorFactory,
                                                  Supplier<StorageEngine> storageEngineSupplier, PageCacheTracer pageCacheTracer,
                                                  LogEntryWriterFactory logEntryWriterFactory, ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
    {
        super( namedDatabaseId, registry, replicator, idGeneratorFactory, RELATIONSHIP_TYPE_TOKEN, storageEngineSupplier, TokenType.RELATIONSHIP,
               TransactionState::relationshipTypeDoCreateForName, pageCacheTracer, logEntryWriterFactory, readOnlyDatabaseChecker );
    }
}
