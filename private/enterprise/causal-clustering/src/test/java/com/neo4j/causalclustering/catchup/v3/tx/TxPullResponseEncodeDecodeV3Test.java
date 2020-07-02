/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.tx;

import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.internal.recordstorage.Command;
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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.internal.kernel.api.security.AuthSubject.ANONYMOUS;
import static org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

class TxPullResponseEncodeDecodeV3Test
{
    private static final int TX_SIZE = sizeOfSingleDecodedTransaction();
    private EmbeddedChannel channel;
    private enum ChunkSize
    {
        LESS_THAN_TX( TX_SIZE / 2 ),
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
    void shouldEncodeAndDecodePullResponseMessage( ChunkSize chunkSize )
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        TxPullResponse sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

        // when
        writeTx( channel, sent );

        // then
        assertInboundEquals( channel, sent );
    }

    @ParameterizedTest( name = "Chunk size {0}" )
    @EnumSource( ChunkSize.class )
    void shouldDecodeTwoTransactionsAfterOneAnther( ChunkSize chunkSize )
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        TxPullResponse sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

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
    void shouldDecodeStreamOfTransactions( ChunkSize chunkSize )
    {
        // given
        channel = new EmbeddedChannel( new TxPullResponseEncoder( chunkSize.chunkSize ), new TxPullResponseDecoder( INSTANCE ) );
        TxPullResponse sent = new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() );

        // when
        writeTx( channel, sent, sent );

        // then
        assertInboundEquals( channel, sent, sent );
    }

    private void assertInboundEquals( EmbeddedChannel channel, TxPullResponse... expected )
    {
        channel.checkException();
        for ( TxPullResponse txPullResponse : expected )
        {
            TxPullResponse readInbound = channel.readInbound();
            assertNotSame( txPullResponse, readInbound );
            assertEquals( txPullResponse, readInbound );
            channel.checkException();
        }

        assertEquals( TxPullResponse.EMPTY, channel.readInbound() );
    }

    private void writeTx( EmbeddedChannel channel, TxPullResponse... sent )
    {
        for ( TxPullResponse txPullResponse : sent )
        {
            channel.writeOutbound( txPullResponse );
        }
        channel.writeOutbound( TxPullResponse.EMPTY );
        ByteBuf chunk;
        while ( (chunk = channel.readOutbound()) != null )
        {
            channel.writeInbound( chunk );
        }
    }

    private static int sizeOfSingleDecodedTransaction()
    {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel( new TxPullResponseEncoder() );
        embeddedChannel.writeOutbound( new TxPullResponse( new StoreId( 1, 2, 3 ), newCommittedTransactionRepresentation() ),
                TxPullResponse.EMPTY );
        ByteBuf byteBuf = embeddedChannel.readOutbound();
        // remove tailing metadata
        var txSize = byteBuf.writerIndex() - Integer.BYTES;
        byteBuf.release();
        return txSize;
    }

    private static CommittedTransactionRepresentation newCommittedTransactionRepresentation()
    {
        final long arbitraryRecordId = 27L;
        Command.NodeCommand command = new Command.NodeCommand( new NodeRecord( arbitraryRecordId ), new NodeRecord( arbitraryRecordId ) );

        PhysicalTransactionRepresentation physicalTransactionRepresentation =
                new PhysicalTransactionRepresentation( singletonList( new LogEntryCommand( command ).getCommand() ) );
        physicalTransactionRepresentation.setHeader( new byte[]{}, 0, 0, 0, 0, ANONYMOUS );

        LogEntryStart startEntry = new LogEntryStart( 0L, 0L, BASE_TX_CHECKSUM, new byte[]{}, LogPosition.UNSPECIFIED );
        LogEntryCommit commitEntry = new LogEntryCommit( 42, 0, 0 );

        return new CommittedTransactionRepresentation( startEntry, physicalTransactionRepresentation, commitEntry );
    }
}
