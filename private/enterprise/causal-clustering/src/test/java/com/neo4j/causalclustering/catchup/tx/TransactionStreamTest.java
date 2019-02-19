/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@SuppressWarnings( {"UnnecessaryLocalVariable"} )
class TransactionStreamTest
{
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final CatchupServerProtocol protocol = mock( CatchupServerProtocol.class );

    private final StoreId storeId = StoreId.UNKNOWN;
    private final ByteBufAllocator allocator = mock( ByteBufAllocator.class );
    private final TransactionCursor cursor = mock( TransactionCursor.class );
    private final int baseTxId = (int) BASE_TX_ID;

    @Test
    void shouldSucceedExactNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 10;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    void shouldSucceedWithNoTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = baseTxId;
        int txIdPromise = baseTxId;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    void shouldSucceedExcessiveNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 9;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @Test
    void shouldFailIncompleteStreamOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 10;
        int txIdPromise = 11;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, E_TRANSACTION_PRUNED );
    }

    @Test
    void shouldSucceedLargeNumberOfTransactions() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 1000;
        int txIdPromise = 900;
        testTransactionStream( firstTxId, lastTxId, txIdPromise, SUCCESS_END_OF_STREAM );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testTransactionStream( int firstTxId, int lastTxId, int txIdPromise, CatchupResult expectedResult ) throws Exception
    {
        TransactionStream txStream =
                new TransactionStream( logProvider.getLog( TransactionStream.class ), new TxPullingContext( cursor, storeId, firstTxId, txIdPromise ),
                        protocol );

        List<Boolean> more = new ArrayList<>();
        List<CommittedTransactionRepresentation> txs = new ArrayList<>();

        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            more.add( true );
            txs.add( tx( txId ) );
        }
        txs.add( null );
        more.add( false );

        when( cursor.next() ).thenAnswer( returnsElementsOf( more ) );
        when( cursor.get() ).thenAnswer( returnsElementsOf( txs ) );

        // when/then
        assertFalse( txStream.isEndOfInput() );

        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            if ( txId == firstTxId )
            {
                assertEquals( ResponseMessageType.TX, txStream.readChunk( allocator ) );
            }
            assertEquals( new TxPullResponse( storeId, txs.get( txId - firstTxId ) ), txStream.readChunk( allocator ) );
        }

        if ( firstTxId <= lastTxId )
        {
            final Object actual = txStream.readChunk( allocator );
            assertEquals( TxPullResponse.V3_END_OF_STREAM_RESPONSE, actual );
        }

        assertEquals( ResponseMessageType.TX_STREAM_FINISHED, txStream.readChunk( allocator ) );
        assertEquals( new TxStreamFinishedResponse( expectedResult, lastTxId ), txStream.readChunk( allocator ) );

        assertTrue( txStream.isEndOfInput() );

        // when
        txStream.close();

        // then
        verify( cursor ).close();
    }

    private CommittedTransactionRepresentation tx( int txId )
    {
        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        when( tx.getCommitEntry() ).thenReturn( new LogEntryCommit( txId, 0 ) );
        return tx;
    }
}
