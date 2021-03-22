/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * A super simple test just to check that the ability to set the bookmark timeout
 * dynamically in the config is not lost during refactoring of neither
 * {@link LocalGraphTransactionIdTracker} nor {@link GraphDatabaseSettings#bookmark_ready_timeout}.
 */
class BookmarkTimeoutDynamicSettingTest
{
    private final TransactionIdTracker transactionIdTracker = mock( TransactionIdTracker.class );
    private LocalGraphTransactionIdTracker graphTransactionIdTracker;
    private Config config;

    @BeforeEach
    void setUp()
    {
        config = Config.newBuilder()
                       .set( GraphDatabaseSettings.bookmark_ready_timeout, Duration.ofSeconds( 1 ) )
                       .build();
        graphTransactionIdTracker = new LocalGraphTransactionIdTracker( transactionIdTracker, null, config );
    }

    @Test
    void testTimeoutChangedDynamically()
    {
        graphTransactionIdTracker.awaitSystemGraphUpToDate( 1001L );
        verify( transactionIdTracker ).awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, 1001L, Duration.ofSeconds( 1 ) );

        config.setDynamic( GraphDatabaseSettings.bookmark_ready_timeout, Duration.ofSeconds( 10 ), "test scope" );

        graphTransactionIdTracker.awaitSystemGraphUpToDate( 1002L );
        verify( transactionIdTracker ).awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, 1002L, Duration.ofSeconds( 10 ) );
    }
}
