/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump.log;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.transaction.log.ArrayIOCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.LEGACY_CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

class TransactionLogEntryCursorTest
{
    @Test
    void shouldDeliverIntactTransactions() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, TX_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND, TX_COMMIT );

        // All transactions

        // The cursor
        TransactionLogEntryCursor transactionCursor = getTransactionLogEntryCursor( tx1, tx2 );

        // THEN
        // tx1
        assertTrue( transactionCursor.next() );
        assertTx( tx1, transactionCursor.get() );

        // tx2
        assertTrue( transactionCursor.next() );
        assertTx( tx2, transactionCursor.get() );

        // No more transactions
        assertFalse( transactionCursor.next() );
    }

    @Test
    void deliverTransactionsWithoutEnd() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, COMMAND, COMMAND, TX_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND );

        TransactionLogEntryCursor transactionCursor = getTransactionLogEntryCursor( tx1, tx2 );

        // THEN
        assertTrue( transactionCursor.next() );
        assertTx( tx1, transactionCursor.get() );

        assertTrue( transactionCursor.next() );
    }

    @Test
    void readNonTransactionalEntries() throws IOException
    {
        List<LogEntry> recordSet1 = makeTransaction( LEGACY_CHECK_POINT, LEGACY_CHECK_POINT, LEGACY_CHECK_POINT );
        List<LogEntry> recordSet2 = makeTransaction( LEGACY_CHECK_POINT );
        TransactionLogEntryCursor transactionCursor = getTransactionLogEntryCursor( recordSet1, recordSet2 );

        for ( int i = 0; i < 4; i++ )
        {
            assertTrue( transactionCursor.next() );
            assertThat( transactionCursor.get() ).hasSize( 1 );
            assertThat( transactionCursor.get()[0].getType() ).isEqualTo( LEGACY_CHECK_POINT );
        }
    }

    private TransactionLogEntryCursor getTransactionLogEntryCursor( List<LogEntry>... txEntries )
    {
        return new TransactionLogEntryCursor( new ArrayIOCursor<>( transactionsAsArray( txEntries ) ) );
    }

    private LogEntry[] transactionsAsArray( List<LogEntry>... transactions )
    {
        return Stream.of( transactions ).flatMap( Collection::stream ).toArray( LogEntry[]::new );
    }

    private void assertTx( List<LogEntry> expected, LogEntry[] actual )
    {
        assertArrayEquals( expected.toArray( new LogEntry[0] ), actual );
    }

    private List<LogEntry> makeTransaction( byte... types )
    {
        List<LogEntry> transaction = new ArrayList<>( types.length );
        for ( Byte type : types )
        {
            transaction.add( mockedLogEntry( type ) );
        }
        return transaction;
    }

    private static LogEntry mockedLogEntry( byte type )
    {
        LogEntry logEntry = mock( LogEntry.class );
        when( logEntry.getType() ).thenReturn( type );
        return logEntry;
    }
}

