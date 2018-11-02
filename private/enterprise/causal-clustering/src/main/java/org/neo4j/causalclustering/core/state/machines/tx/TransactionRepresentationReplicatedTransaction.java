/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.WritableChannel;

public class TransactionRepresentationReplicatedTransaction implements ReplicatedTransaction
{
    private final TransactionRepresentation tx;

    TransactionRepresentationReplicatedTransaction( TransactionRepresentation tx )
    {
        this.tx = tx;
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return ReplicatedTransactionSerializer.encode( this );
    }

    @Override
    public void marshal( WritableChannel writableChannel ) throws IOException
    {
        ReplicatedTransactionSerializer.marshal( writableChannel, this );
    }

    @Override
    public TransactionRepresentation extract( TransactionRepresentationExtractor extractor )
    {
        return extractor.extract( this );
    }

    public TransactionRepresentation tx()
    {
        return tx;
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }
}
