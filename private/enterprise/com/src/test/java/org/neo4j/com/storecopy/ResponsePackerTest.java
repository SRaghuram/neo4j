/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.StoreIdTestFactory;
import org.neo4j.function.Suppliers;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

public class ResponsePackerTest
{
    @Test
    public void shouldHaveFixedTargetTransactionIdEvenIfLastTransactionIdIsMoving() throws Exception
    {
        // GIVEN
        LogicalTransactionStore transactionStore = mock( LogicalTransactionStore.class );
        long lastAppliedTransactionId = 5L;
        TransactionCursor endlessCursor = new EndlessCursor( lastAppliedTransactionId + 1 );
        when( transactionStore.getTransactions( anyLong() ) ).thenReturn( endlessCursor );
        final long targetTransactionId = 8L;
        final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore( targetTransactionId, 0,
                BASE_TX_COMMIT_TIMESTAMP, 0, 0 );
        ResponsePacker packer = new ResponsePacker( transactionStore, transactionIdStore,
                Suppliers.singleton( StoreIdTestFactory.newStoreIdForCurrentVersion() ) );

        // WHEN
        Response<Object> response = packer.packTransactionStreamResponse( requestContextStartingAt( 5L ), null );
        final AtomicLong nextExpectedVisit = new AtomicLong( lastAppliedTransactionId );
        response.accept( new Response.Handler()
        {
            @Override
            public void obligation( long txId )
            {
                fail( "Should not be called" );
            }

            @Override
            public Visitor<CommittedTransactionRepresentation,Exception> transactions()
            {
                return element ->
                {
                    // THEN
                    long txId = element.getCommitEntry().getTxId();
                    assertThat( txId, lessThanOrEqualTo( targetTransactionId ) );
                    assertEquals( nextExpectedVisit.incrementAndGet(), txId );

                    // Move the target transaction id forward one step, effectively always keeping it out of reach
                    transactionIdStore.setLastCommittedAndClosedTransactionId(
                            transactionIdStore.getLastCommittedTransactionId() + 1, 0, BASE_TX_COMMIT_TIMESTAMP,
                            3, 4 );
                    return true;
                };
            }
        } );
    }

    private RequestContext requestContextStartingAt( long txId )
    {
        return new RequestContext( 0, 0, 0, txId, 0 );
    }

    public class EndlessCursor implements TransactionCursor
    {
        private final LogPosition position = new LogPosition( 0, 0 );
        private long txId;
        private CommittedTransactionRepresentation transaction;

        public EndlessCursor( long txId )
        {
            this.txId = txId;
        }

        @Override
        public void close()
        {
        }

        @Override
        public CommittedTransactionRepresentation get()
        {
            return transaction;
        }

        @Override
        public boolean next()
        {
            transaction = new CommittedTransactionRepresentation( null, null,
                    new LogEntryCommit( txId++, 0 ) );
            return true;
        }

        @Override
        public LogPosition position()
        {
            return position;
        }
    }
}
