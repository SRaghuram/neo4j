/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.messaging.marshalling.ByteArrayTransactionChunker;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.OptionalLong;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * A transaction represented as an immutable byte array.
 */
public class ByteArrayReplicatedTransaction extends ReplicatedTransaction
{
    private final byte[] txBytes;
    private final DatabaseId databaseId;

    @Override
    public OptionalLong size()
    {
        return OptionalLong.of( txBytes.length );
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    ByteArrayReplicatedTransaction( byte[] txBytes, DatabaseId databaseId )
    {
        super( databaseId );
        this.txBytes = txBytes;
        this.databaseId = databaseId;
    }

    public byte[] getTxBytes()
    {
        return txBytes;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ByteArrayReplicatedTransaction that = (ByteArrayReplicatedTransaction) o;
        return Arrays.equals( txBytes, that.txBytes );
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( txBytes );
    }

    @Override
    public ChunkedInput<ByteBuf> encode()
    {
        return new ByteArrayTransactionChunker( this );
    }

    @Override
    public TransactionRepresentation extract( TransactionRepresentationExtractor extractor )
    {
        return extractor.extract( this );
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    @Override
    public String toString()
    {
        return "ByteArrayReplicatedTransaction{" + "txBytes.length=" + txBytes.length + '}';
    }
}
