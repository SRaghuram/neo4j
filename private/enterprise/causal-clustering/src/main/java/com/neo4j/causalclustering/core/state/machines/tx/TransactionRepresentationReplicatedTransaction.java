/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * The regular transaction in the POJO format represented by the kernel.
 */
public class TransactionRepresentationReplicatedTransaction extends ReplicatedTransaction
{
    private final TransactionRepresentation tx;
    private final DatabaseId databaseId;
    private final LogEntryWriterFactory logEntryWriterFactory;

    public TransactionRepresentationReplicatedTransaction( TransactionRepresentation tx, DatabaseId databaseId, LogEntryWriterFactory logEntryWriterFactory )
    {
        super( databaseId );
        this.tx = tx;
        this.databaseId = databaseId;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return new ChunkedTransaction( this, logEntryWriterFactory );
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
