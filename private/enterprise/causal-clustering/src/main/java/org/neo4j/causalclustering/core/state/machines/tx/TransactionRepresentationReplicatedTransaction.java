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

/**
 * The regular transaction in the POJO format represented by the kernel.
 */
public class TransactionRepresentationReplicatedTransaction extends ReplicatedTransaction
{
    private final TransactionRepresentation tx;
    private final String databaseName;

    public TransactionRepresentationReplicatedTransaction( TransactionRepresentation tx, String databaseName )
    {
        super( databaseName );
        this.tx = tx;
        this.databaseName = databaseName;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return new ChunkedTransaction( this );
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
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    @Override
    public String toString()
    {
        return "TransactionRepresentationReplicatedTransaction{" + "tx=" + tx + '}';
    }
}
