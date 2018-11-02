/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.replication.RaftReplicator;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.storageengine.api.StorageEngine;

import static org.neo4j.causalclustering.core.state.machines.token.TokenType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;

public class ReplicatedRelationshipTypeTokenHolder extends ReplicatedTokenHolder
{
    public ReplicatedRelationshipTypeTokenHolder( TokenRegistry registry, RaftReplicator replicator, IdGeneratorFactory idGeneratorFactory,
            Supplier<StorageEngine> storageEngineSupplier )
    {
        super( registry, replicator, idGeneratorFactory, RELATIONSHIP_TYPE_TOKEN, storageEngineSupplier, RELATIONSHIP,
                TransactionState::relationshipTypeDoCreateForName );
    }
}
