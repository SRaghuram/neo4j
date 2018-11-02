/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.WritableChannel;

public interface ReplicatedTransaction extends CoreReplicatedContent
{
    static TransactionRepresentationReplicatedTransaction from( TransactionRepresentation tx )
    {
        return new TransactionRepresentationReplicatedTransaction( tx );
    }

    static ByteArrayReplicatedTransaction from( byte[] bytes )
    {
        return new ByteArrayReplicatedTransaction( bytes );
    }

    @Override
    default void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    ChunkedInput<ByteBuf> encode();

    void marshal( WritableChannel writableChannel ) throws IOException;

    TransactionRepresentation extract( TransactionRepresentationExtractor extractor );
}
