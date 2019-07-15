/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.extension.SuppressOutputExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( SuppressOutputExtension.class )
class ReconciledTransactionIdTrackerTest
{
    private ReconciledTransactionIdTracker tracker;

    @BeforeEach
    void beforeEach()
    {
        tracker = new ReconciledTransactionIdTracker( FormattedLogProvider.toOutputStream( System.out ) );
    }

    @Test
    void shouldReturnDummyReconciledTransactionIdWhenNotInitialized()
    {
        assertEquals( -1, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenInitializedButNeverUpdated()
    {
        tracker.initialize( 42 );

        assertEquals( 42, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenReInitialized()
    {
        tracker.initialize( 42 );
        tracker.initialize( 4242 );
        tracker.initialize( 424242 );

        assertEquals( 424242, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldReturnReconciledTransactionIdWhenInitializedAndUpdated()
    {
        tracker.initialize( 1 );

        tracker.setLastReconciledTransactionId( 7 );
        tracker.setLastReconciledTransactionId( 2 );
        tracker.setLastReconciledTransactionId( 3 );
        tracker.setLastReconciledTransactionId( 5 );
        tracker.setLastReconciledTransactionId( 4 );

        assertEquals( 5, tracker.getLastReconciledTransactionId() );
    }

    @Test
    void shouldFailToInitializeWithNegativeTransactionId()
    {
        assertThrows( IllegalArgumentException.class, () -> tracker.initialize( -42 ) );
    }

    @Test
    void shouldFailToUpdateWithNegativeTransactionId()
    {
        tracker.initialize( 42 );

        assertThrows( IllegalArgumentException.class, () -> tracker.setLastReconciledTransactionId( -42 ) );
    }

    @Test
    void shouldFailToUpdateWhenNotInitialized()
    {
        assertThrows( IllegalStateException.class, () -> tracker.setLastReconciledTransactionId( 42 ) );
    }

    @Test
    void shouldFailToUpdateWithNonIncreasingTransactionId()
    {
        tracker.initialize( 1 );

        tracker.setLastReconciledTransactionId( 2 );
        tracker.setLastReconciledTransactionId( 3 );
        tracker.setLastReconciledTransactionId( 4 );

        assertThrows( IllegalArgumentException.class, () -> tracker.setLastReconciledTransactionId( 2 ) );
    }
}
