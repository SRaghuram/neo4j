/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dump.log;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.transaction.log.ArrayIOCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.COMMAND;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_1P_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.TX_START;

public class TransactionLogEntryCursorTest
{
    @Test
    public void shouldDeliverIntactTransactions() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, TX_1P_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND, TX_1P_COMMIT );

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
    public void deliverTransactionsWithoutEnd() throws IOException
    {
        // GIVEN
        // tx 1
        List<LogEntry> tx1 = makeTransaction( TX_START, COMMAND, COMMAND, COMMAND, TX_1P_COMMIT );

        // tx 2
        List<LogEntry> tx2 = makeTransaction( TX_START, COMMAND, COMMAND );

        TransactionLogEntryCursor transactionCursor = getTransactionLogEntryCursor( tx1, tx2 );

        // THEN
        assertTrue( transactionCursor.next() );
        assertTx( tx1, transactionCursor.get() );

        assertTrue( transactionCursor.next() );
    }

    @Test
    public void readNonTransactionalEntries() throws IOException
    {
        List<LogEntry> recordSet1 = makeTransaction( CHECK_POINT, CHECK_POINT, CHECK_POINT );
        List<LogEntry> recordSet2 = makeTransaction( CHECK_POINT );
        TransactionLogEntryCursor transactionCursor = getTransactionLogEntryCursor( recordSet1, recordSet2 );

        assertTrue( transactionCursor.next() );
        assertThat( "All 4 checkpoints should be provided.", transactionCursor.get(), arrayWithSize( 4 ) );
    }

    private TransactionLogEntryCursor getTransactionLogEntryCursor( List<LogEntry>...txEntries )
    {
        return new TransactionLogEntryCursor( new ArrayIOCursor( transactionsAsArray( txEntries ) ) );
    }

    private LogEntry[] transactionsAsArray( List<LogEntry>... transactions )
    {
        return Stream.of( transactions ).flatMap( Collection::stream ).toArray( LogEntry[]::new );
    }

    private void assertTx( List<LogEntry> expected, LogEntry[] actual )
    {
        assertArrayEquals( expected.toArray( new LogEntry[expected.size()] ), actual );
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

