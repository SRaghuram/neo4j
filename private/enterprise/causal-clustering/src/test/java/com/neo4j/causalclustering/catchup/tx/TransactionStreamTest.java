/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static com.neo4j.causalclustering.catchup.CatchupResult.E_GENERAL_ERROR;
import static com.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@SuppressWarnings( {"UnnecessaryLocalVariable"} )
class TransactionStreamTest
{
    private final Log log = NullLogProvider.getInstance().getLog( TransactionStream.class );
    private final CatchupServerProtocol protocol = mock( CatchupServerProtocol.class );

    private final StoreId storeId = StoreId.UNKNOWN;
    private final NamedDatabaseId databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private final ByteBufAllocator allocator = mock( ByteBufAllocator.class );
    private final TransactionCursor cursor = mock( TransactionCursor.class );
    private final int baseTxId = (int) BASE_TX_ID;
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private TxStreamingConstraint constraint = new TxStreamingConstraint.Unbounded();

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

    @Test
    void shouldRespectLimitedConstraint() throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = 100;
        int lastCommitedTxId = 10;
        var txs = prepareCursor( firstTxId, lastTxId, null, null );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId + 1 );

        var txStream = new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, firstTxId,
                lastCommitedTxId, transactionIdStore, LogEntryWriterFactory.LATEST, new TxStreamingConstraint.Limited( lastCommitedTxId ) ), protocol );
        var expectedChucks =
                prepareExpectedElements( firstTxId, lastCommitedTxId, txs.stream().filter( tx -> tx.getCommitEntry().getTxId() <= lastCommitedTxId ).collect(
                        Collectors.toList() ), SUCCESS_END_OF_STREAM );

        // end of input is only false because thrown exception is also queued as chunk, just to be thrown is reached
        assertExpectedElements( txStream, expectedChucks, true );
        // the exception should have been the last chuck
        assertTrue( txStream.isEndOfInput() );
    }

    @ParameterizedTest( name = "txCountBeforeException={0}" )
    @ValueSource( ints = {0, 1, 2} )
    void shouldFailOnCursorNextException( int count ) throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = baseTxId + count;
        var exception = new Exception();
        var txs = prepareCursor( firstTxId, lastTxId, exception, null );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId + 1 );

        var txStream = new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, firstTxId,
                lastTxId, transactionIdStore, LogEntryWriterFactory.LATEST, constraint ), protocol );
        var expectedChucks = prepareExpectedElements( firstTxId, lastTxId, txs, E_GENERAL_ERROR );

        // end of input is only false because thrown exception is also queued as chunk, just to be thrown is reached
        assertExpectedElements( txStream, expectedChucks, false );
        var thrownException = assertThrows( Throwable.class, () -> txStream.readChunk( allocator ) );
        assertEquals( exception, thrownException );
        // the exception should have been the last chuck
        assertTrue( txStream.isEndOfInput() );
    }

    @ParameterizedTest( name = "txCountBeforeException={0}" )
    @ValueSource( ints = {0, 1, 2} )
    void shouldFailOnCursorGetException( int count ) throws Exception
    {
        int firstTxId = baseTxId;
        int lastTxId = baseTxId + count;
        var exception = new Exception();
        var txs = prepareCursor( firstTxId, lastTxId, null, exception );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId + 1 );

        var txStream = new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, firstTxId,
                lastTxId, transactionIdStore, LogEntryWriterFactory.LATEST, constraint ), protocol );
        var expectedChucks = prepareExpectedElements( firstTxId, lastTxId, txs, E_GENERAL_ERROR );

        // end of input is only false because thrown exception is also queued as chunk, just to be thrown is reached
        assertExpectedElements( txStream, expectedChucks, false );
        var thrownException = assertThrows( Throwable.class, () -> txStream.readChunk( allocator ) );
        assertEquals( exception, thrownException );
        // the exception should have been the last chuck
        assertTrue( txStream.isEndOfInput() );
    }

    @Test
    void shouldFailOnNonConsecutiveTransactions() throws Exception
    {
        var lastTxId = 1000;
        var tx1 = tx( lastTxId - 1 );
        var tx2 = tx( lastTxId + 1 );

        when( cursor.next() ).thenReturn( true, true, false );
        when( cursor.get() ).thenReturn( tx1, tx2, null );

        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId );

        var txStream = new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, lastTxId - 1,
                lastTxId, transactionIdStore, LogEntryWriterFactory.LATEST, constraint ), protocol );
        var expectedChucks = List.of( ResponseMessageType.TX, writable( new TxPullResponse( storeId, tx1 ) ),
                ResponseMessageType.TX_STREAM_FINISHED, new TxStreamFinishedResponse( E_GENERAL_ERROR, lastTxId + 1 ) );

        // end of input is only false because thrown exception is also queued as chunk, just to be thrown is reached
        assertExpectedElements( txStream, expectedChucks, false );
        var thrownException = assertThrows( IllegalStateException.class, () -> txStream.readChunk( allocator ) );
        assertEquals( thrownException.getMessage(), "Transaction cursor out of order. Expected 1000 but was 1001" );
    }

    @Test
    void shouldNotContinueIfTransactionIdStoreDoesNotAllowIt() throws Exception
    {
        var lastTxId = 2;
        var firstTxId = 1;
        var txPromise = 1;
        var tx1 = tx( firstTxId );
        var tx2 = tx( firstTxId + 1 );

        when( cursor.next() ).thenReturn( true );
        when( cursor.get() ).thenReturn( tx1 ).thenReturn( tx2 ).thenThrow( new IllegalStateException( "Should never try to get this transaction" ) );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId );

        var transactionStream =
                new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, firstTxId,
                        txPromise, transactionIdStore, LogEntryWriterFactory.LATEST, constraint ), protocol );
        var expectedChucks = prepareExpectedElements( firstTxId, lastTxId, List.of( tx1, tx2 ), SUCCESS_END_OF_STREAM );

        assertExpectedElements( transactionStream, expectedChucks, true );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testTransactionStream( int firstTxId, int lastTxId, int txIdPromise, CatchupResult expectedResult ) throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( (long) lastTxId );
        var txStream = new TransactionStream( log, new TxPullingContext( cursor, storeId, databaseId, firstTxId,
                txIdPromise, transactionIdStore, LogEntryWriterFactory.LATEST, constraint ), protocol );
        var txs = prepareCursor( firstTxId, lastTxId );
        var expectedElements = prepareExpectedElements( firstTxId, lastTxId, txs, expectedResult );

        // when/then
        assertExpectedElements( txStream, expectedElements, true );

        // when
        txStream.close();

        // then
        verify( cursor ).close();
    }

    private List<CommittedTransactionRepresentation> prepareCursor( int firstTxId, int lastTxId ) throws Exception
    {
        return prepareCursor( firstTxId, lastTxId, null, null );
    }

    private List<CommittedTransactionRepresentation> prepareCursor( int firstTxId, int lastTxId, Exception exceptionOnNext, Exception exceptionOnGet )
            throws Exception
    {
        List<Object> more = new ArrayList<>();
        List<CommittedTransactionRepresentation> txs = new ArrayList<>();

        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            more.add( true );
            txs.add( tx( txId ) );
        }
        Object closingNextElement = exceptionOnNext != null ? exceptionOnNext : (exceptionOnGet != null);

        when( cursor.next() ).thenAnswer( new ReturnOrThrowElementsOf( more, closingNextElement ) );
        when( cursor.get() ).thenAnswer( new ReturnOrThrowElementsOf( txs, exceptionOnGet ) );
        return txs;
    }

    private ArrayList<Object> prepareExpectedElements( int firstTxId, int lastTxId, List<CommittedTransactionRepresentation> txs, CatchupResult expectedResult )
    {
        var expectedElements = new ArrayList<>();
        for ( int txId = firstTxId; txId <= lastTxId; txId++ )
        {
            if ( txId == firstTxId )
            {
                expectedElements.add( ResponseMessageType.TX );
            }
            expectedElements.add( writable( new TxPullResponse( storeId, txs.get( txId - firstTxId ) ) ) );
        }
        if ( firstTxId <= lastTxId && expectedResult != E_GENERAL_ERROR )
        {
            expectedElements.add( writable( TxPullResponse.EMPTY ) );
        }
        expectedElements.add( ResponseMessageType.TX_STREAM_FINISHED );
        expectedElements.add( new TxStreamFinishedResponse( expectedResult, lastTxId ) );
        return expectedElements;
    }

    private void assertExpectedElements( TransactionStream txStream, List<Object> expectedElements, boolean shouldBeEnded ) throws Exception
    {
        for ( var expectedChunk : expectedElements )
        {
            assertFalse( txStream.isEndOfInput() );
            var actual = txStream.readChunk( allocator );
            assertEquals( expectedChunk, actual );
        }
        assertEquals( txStream.isEndOfInput(), shouldBeEnded );
    }

    private CommittedTransactionRepresentation tx( int txId )
    {
        var tx = mock( CommittedTransactionRepresentation.class );
        when( tx.getCommitEntry() ).thenReturn( new LogEntryCommit( txId, 0, BASE_TX_CHECKSUM ) );
        return tx;
    }
    private WritableTxPullResponse writable( TxPullResponse txPullResponse )
    {
        return new WritableTxPullResponse( txPullResponse, LogEntryWriterFactory.LATEST );
    }

    private static class ReturnOrThrowElementsOf implements Answer<Object>
    {
        private final LinkedList<Object> elements;

        ReturnOrThrowElementsOf( List<?> elements, Object closingElement )
        {
            this.elements = new LinkedList<>( elements );
            this.elements.add( closingElement );
        }

        @Override
        public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
        {
            Object element = elements.poll();
            if ( element instanceof Throwable )
            {
                throw (Throwable) element;
            }
            else
            {
                return element;
            }
        }
    }
}
