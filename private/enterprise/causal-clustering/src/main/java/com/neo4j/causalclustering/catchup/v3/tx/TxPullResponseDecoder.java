/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.ReadableNetworkChannelDelegator;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;

public class TxPullResponseDecoder extends ByteToMessageDecoder
{
    private PhysicalTransactionCursor transactionCursor;
    private NextTxInfo nextTxInfo;
    private StoreId storeId;
    private final LogEntryReader reader;
    private final ReadableNetworkChannelDelegator delegatingChannel = new ReadableNetworkChannelDelegator();

    public TxPullResponseDecoder( CommandReaderFactory commandReaderFactory )
    {
        reader = new VersionAwareLogEntryReader( commandReaderFactory );
        setCumulator( COMPOSITE_CUMULATOR );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        delegatingChannel.delegateTo( new NetworkReadableChannel( in ) );
        if ( isFirstChunk() )
        {
            storeId = StoreIdMarshal.INSTANCE.unmarshal( delegatingChannel );
            transactionCursor = new PhysicalTransactionCursor( delegatingChannel, reader );
            nextTxInfo = new NextTxInfo();
        }

        while ( nextTxInfo.canReadNextTx( in ) )
        {
            var txStartIndex = in.readerIndex();
            transactionCursor.next();
            var txSize = in.readerIndex() - txStartIndex;
            CommittedTransactionRepresentation tx = transactionCursor.get();
            out.add( new ReceivedTxPullResponse( storeId, tx, txSize ) );
            nextTxInfo.update( in );
        }

        if ( nextTxInfo.noMoreTx() )
        {
            transactionCursor.close();
            transactionCursor = null;
            nextTxInfo = null;
            out.add( ReceivedTxPullResponse.EMPTY );
        }
    }

    private boolean isFirstChunk()
    {
        return transactionCursor == null;
    }

    private static class NextTxInfo
    {
        private boolean unknown;
        private int nextSize;

        private NextTxInfo()
        {
            this.unknown = true;
        }

        boolean canReadNextTx( ByteBuf byteBuf )
        {
            if ( unknown )
            {
                update( byteBuf );
            }
            return !unknown && nextSize < byteBuf.readableBytes();
        }

        void update( ByteBuf byteBuf )
        {
            unknown = !byteBuf.isReadable();
            nextSize = unknown ? 0 : byteBuf.readInt();
        }

        boolean noMoreTx()
        {
            return !unknown && nextSize == 0;
        }
    }
}
