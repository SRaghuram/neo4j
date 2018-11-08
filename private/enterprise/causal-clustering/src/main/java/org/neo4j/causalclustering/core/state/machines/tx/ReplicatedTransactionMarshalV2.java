/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.causalclustering.messaging.ByteBufBacked;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory.transactionalRepresentationWriter;

public class ReplicatedTransactionMarshalV2
{
    private ReplicatedTransactionMarshalV2()
    {
    }

    public static void marshal( WritableChannel writableChannel, ByteArrayReplicatedTransaction replicatedTransaction ) throws IOException
    {
        StringMarshal.marshal( writableChannel, replicatedTransaction.databaseName() );
        int length = replicatedTransaction.getTxBytes().length;
        writableChannel.putInt( length );
        writableChannel.put( replicatedTransaction.getTxBytes(), length );
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException
    {
        String databaseName = StringMarshal.unmarshal( channel );
        int txBytesLength = channel.getInt();
        byte[] txBytes = new byte[txBytesLength];
        channel.get( txBytes, txBytesLength );
        return ReplicatedTransaction.from( txBytes, databaseName );
    }

    public static void marshal( WritableChannel writableChannel, TransactionRepresentationReplicatedTransaction replicatedTransaction ) throws IOException
    {
        StringMarshal.marshal( writableChannel, replicatedTransaction.databaseName() );
        if ( writableChannel instanceof ByteBufBacked )
        {
            /*
             * Marshals more efficiently if Channel is going over the network. In practice, this means maintaining support for
             * RaftV1 without loosing performance
             */
            ByteBuf buffer = ((ByteBufBacked) writableChannel).byteBuf();
            int metaDataIndex = buffer.writerIndex();
            int txStartIndex = metaDataIndex + Integer.BYTES;
            // leave room for length to be set later.
            buffer.writerIndex( txStartIndex );
            writeTx( writableChannel, replicatedTransaction.tx() );
            int txLength = buffer.writerIndex() - txStartIndex;
            buffer.setInt( metaDataIndex, txLength );
        }
        else
        {
            /*
             * Unknown length. This should only be reached in tests. When a ReplicatedTransaction is marshaled to file it has already passed over the network
             * and is of a different type. More efficient marshalling is used in ByteArrayReplicatedTransaction.
             */
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream( 1024 );
            OutputStreamWritableChannel outputStreamWritableChannel = new OutputStreamWritableChannel( outputStream );
            writeTx( outputStreamWritableChannel, replicatedTransaction.tx() );
            int length = outputStream.size();
            writableChannel.putInt( length );
            writableChannel.put( outputStream.toByteArray(), length );
        }
    }

    private static void writeTx( WritableChannel writableChannel, TransactionRepresentation tx ) throws IOException
    {
        ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter = transactionalRepresentationWriter( tx );
        while ( txWriter.canWrite() )
        {
            txWriter.write( writableChannel );
        }
    }
}
