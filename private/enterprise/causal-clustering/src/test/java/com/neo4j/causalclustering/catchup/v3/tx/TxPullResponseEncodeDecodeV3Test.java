/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.WritableTxPullResponse;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.storageengine.api.StoreId;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

class TxPullResponseEncodeDecodeV3Test
{
    private static final int TX_SIZE = sizeOfSingleDecodedTransaction();

    private EmbeddedChannel channel;

    private enum ChunkSize
    {
        LESS_THAN_TX( TX_SIZE - 10 ),
        MORE_THAN_TX_EVEN( TX_SIZE * 2 ),
        MORE_THAN_TX_UNEVEN( (int) (TX_SIZE * 1.5) ),
        EQUAL_TO_TX( TX_SIZE );

        private final int chunkSize;

        ChunkSize( int chunkSize )
        {
            this.chunkSize = chunkSize;
        }
    }

    @AfterEach
    void completeChannel()
    {
        channel.finishAndReleaseAll();
        channel = null;
    }

    @ParameterizedTest( name = "Chunk size {0}" )
    @EnumSource( ChunkSize.class )
    void shouldEncodeAndDecodePullResponseMessage( ChunkSize chunkSize ) throws IOException
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        var sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

        // when
        writeTx( channel, sent );

        // then
        assertInboundEquals( channel, sent );
    }

    @ParameterizedTest( name = "Chunk size {0}" )
    @EnumSource( ChunkSize.class )
    void shouldDecodeTwoTransactionsAfterOneAnther( ChunkSize chunkSize ) throws IOException
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        var sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

        // when
        writeTx( channel, sent );

        // then
        assertInboundEquals( channel, sent );

        // when
        writeTx( channel, sent );

        // then
        assertInboundEquals( channel, sent );
    }

    @ParameterizedTest( name = "Chunk size {0}" )
    @EnumSource( ChunkSize.class )
    void shouldDecodeStreamOfTransactions( ChunkSize chunkSize ) throws IOException
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        var sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

        // when
        writeTx( channel, sent, sent );

        // then
        assertInboundEquals( channel, sent, sent );
    }

    private void assertInboundEquals( EmbeddedChannel channel, TxPullResponse... expected ) throws IOException
    {
        channel.checkException();
        for ( var txPullResponse : expected )
        {
            ReceivedTxPullResponse readInbound = channel.readInbound();
            assertEquals( new ReceivedTxPullResponse( txPullResponse.storeId(), txPullResponse.tx(), actualTxSize() ), readInbound );
            channel.checkException();
        }

        assertEquals( ReceivedTxPullResponse.EMPTY, channel.readInbound() );
    }

    private void writeTx( EmbeddedChannel channel, TxPullResponse... sent )
    {
        for ( var txPullResponse : sent )
        {
            channel.writeOutbound( writable( txPullResponse ) );
        }
        channel.writeOutbound( writable( TxPullResponse.EMPTY ) );
        ByteBuf chunk;
        while ( (chunk = channel.readOutbound()) != null )
        {
            channel.writeInbound( chunk );
        }
    }

    private int actualTxSize() throws IOException
    {
        var inMemoryChannel = new NetworkWritableChannel( Unpooled.buffer() );
        var tx = newCommittedTransactionRepresentation();
        var entryWriter = LogEntryWriterFactory.LATEST.createEntryWriter( inMemoryChannel );
        entryWriter.serialize( tx );
        return inMemoryChannel.byteBuf().readableBytes();
    }

    private static int sizeOfSingleDecodedTransaction()
    {
        var embeddedChannel = new EmbeddedChannel( new TxPullResponseEncoder() );
        var txPullResopnse = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );
        embeddedChannel.writeOutbound( writable( txPullResopnse ), writable( TxPullResponse.EMPTY ) );
        ByteBuf byteBuf = embeddedChannel.readOutbound();
        // remove tailing metadata
        var txSize = byteBuf.writerIndex() - Integer.BYTES;
        byteBuf.release();
        return txSize;
    }

    private static WritableTxPullResponse writable( TxPullResponse txPullResponse )
    {
        return new WritableTxPullResponse( txPullResponse, LogEntryWriterFactory.LATEST );
    }

    private static CommittedTransactionRepresentation newCommittedTransactionRepresentation()
    {
        final var arbitraryRecordId = 27L;
        var command = new Command.NodeCommand( new NodeRecord( arbitraryRecordId ), new NodeRecord( arbitraryRecordId ) );

        var physicalTransactionRepresentation =
                new PhysicalTransactionRepresentation( singletonList( new LogEntryCommand( command ).getCommand() ) );
        physicalTransactionRepresentation.setHeader( new byte[]{}, 0, 0, 0, 0, ANONYMOUS );

        var startEntry = new LogEntryStart( 0L, 0L, BASE_TX_CHECKSUM, new byte[]{}, LogPosition.UNSPECIFIED );
        var commitEntry = new LogEntryCommit( 42, 0, 0 );

        return new CommittedTransactionRepresentation( startEntry, physicalTransactionRepresentation, commitEntry );
    }
}
