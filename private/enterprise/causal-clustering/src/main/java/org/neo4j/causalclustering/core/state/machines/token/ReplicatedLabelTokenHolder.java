/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.storageengine.api.StorageEngine;

public class ReplicatedLabelTokenHolder extends ReplicatedTokenHolder
{
    public ReplicatedLabelTokenHolder( TokenRegistry registry, Replicator replicator,
            IdGeneratorFactory idGeneratorFactory, Supplier<StorageEngine> storageEngineSupplier )
    {
        super( registry, replicator, idGeneratorFactory, IdType.LABEL_TOKEN, storageEngineSupplier, TokenType.LABEL, TransactionState::labelDoCreateForName );
    }
}
