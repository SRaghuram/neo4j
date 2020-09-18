/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.ByteBufBacked;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public class ReplicatedTransactionMarshalV2
{
    private ReplicatedTransactionMarshalV2()
    {
    }

    public static void marshal( WritableChannel writableChannel, ByteArrayReplicatedTransaction replicatedTransaction )
            throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( replicatedTransaction.databaseId(), writableChannel );
        int length = replicatedTransaction.getTxBytes().length;
        writableChannel.putInt( length );
        writableChannel.put( replicatedTransaction.getTxBytes(), length );
    }

    public static ReplicatedTransaction unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
        int txBytesLength = channel.getInt();
        byte[] txBytes = new byte[txBytesLength];
        channel.get( txBytes, txBytesLength );
        return ReplicatedTransaction.from( txBytes, databaseId );
    }

    public static void marshal( WritableChannel writableChannel, TransactionRepresentationReplicatedTransaction replicatedTransaction,
                                LogEntryWriterFactory logEntryWriterFactory ) throws IOException
    {
        DatabaseIdWithoutNameMarshal.INSTANCE.marshal( replicatedTransaction.databaseId(), writableChannel );
        if ( (writableChannel instanceof ByteBufBacked) && (writableChannel instanceof WritableChecksumChannel) )
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
            writeTx( (WritableChecksumChannel) writableChannel, replicatedTransaction.tx(), logEntryWriterFactory );
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
            writeTx( outputStreamWritableChannel, replicatedTransaction.tx(), logEntryWriterFactory );
            int length = outputStream.size();
            writableChannel.putInt( length );
            writableChannel.put( outputStream.toByteArray(), length );
        }
    }

    private static void writeTx( WritableChecksumChannel writableChannel, TransactionRepresentation tx,
                                 LogEntryWriterFactory logEntryWriterFactory ) throws IOException
    {
        ReplicatedTransactionFactory.TransactionRepresentationWriter txWriter =
                ReplicatedTransactionFactory.transactionalRepresentationWriter( tx, logEntryWriterFactory );
        while ( txWriter.canWrite() )
        {
            txWriter.write( writableChannel );
        }
    }
}
