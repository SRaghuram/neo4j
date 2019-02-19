/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.ReadableNetworkChannelDelegator;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ServiceLoadingCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StoreId;

public class TxPullResponseDecoder extends ByteToMessageDecoder
{

    private PhysicalTransactionCursor<ReadableNetworkChannelDelegator> transactionCursor;
    private int nextTxSize;
    private boolean readNextTxSize;
    private StoreId storeId;
    private LogEntryReader<ReadableNetworkChannelDelegator> reader =
            new VersionAwareLogEntryReader<>( new ServiceLoadingCommandReaderFactory(), InvalidLogEntryHandler.STRICT );
    private ReadableNetworkChannelDelegator delegatingChannel = new ReadableNetworkChannelDelegator();

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        delegatingChannel.delegateTo( new NetworkReadableClosableChannelNetty4( in ) );
        if ( isFirstChunk() )
        {
            storeId = StoreIdMarshal.INSTANCE.unmarshal( delegatingChannel );
            transactionCursor = new PhysicalTransactionCursor<>( delegatingChannel, reader );
            readNextTxSize = true;
        }

        if ( readNextTxSize )
        {
            nextTxSize = in.readInt();
            readNextTxSize = false;
        }
        while ( nextTxSize < in.readableBytes() )
        {
            transactionCursor.next();
            CommittedTransactionRepresentation tx = transactionCursor.get();
            out.add( new TxPullResponse( storeId, tx ) );
            // More data is coming in next buffer. Avoid reading out of bounds.
            if ( !in.isReadable() )
            {
                readNextTxSize = true;
                return;
            }
            nextTxSize = in.readInt();
        }

        if ( noMoreTransactions() )
        {
            transactionCursor.close();
            transactionCursor = null;
            out.add( TxPullResponse.V3_END_OF_STREAM_RESPONSE );
        }
    }

    private boolean noMoreTransactions()
    {
        return nextTxSize == 0;
    }

    private boolean isFirstChunk()
    {
        return transactionCursor == null;
    }
}
