/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class TxPullResponseHandlerTest
{
    @Test
    void shouldNotSignalCompleteOnTxPullResponse()
    {
        // given
        var catchupProcess = mock( CatchupPollingProcess.class );
        var handler = new TxPullResponseHandler( catchupProcess, NullLog.getInstance() );
        var signal = new CompletableFuture<TxStreamFinishedResponse>();

        // when
        handler.onTxPullResponse( signal, mock( TxPullResponse.class ) );

        // then
        verify( catchupProcess ).handleTransaction( any() );
        assertFalse( signal.isDone() );
    }

    @Test
    void shouldSignalCompleteOnTxPullResponseIfProcessStopped()
    {
        // given
        var catchupProcess = mock( CatchupPollingProcess.class );
        when( catchupProcess.isCancelled() ).thenReturn( true );
        var handler = new TxPullResponseHandler( catchupProcess, NullLog.getInstance() );
        var signal = new CompletableFuture<TxStreamFinishedResponse>();

        // when
        handler.onTxPullResponse( signal, mock( TxPullResponse.class ) );

        // then
        assertTrue( signal.isDone() );
    }

    @Test
    void shouldSignalCompleteOnTxStreamFinishedResponse()
    {
        // given
        var catchupProcess = mock( CatchupPollingProcess.class );
        var handler = new TxPullResponseHandler( catchupProcess, NullLog.getInstance() );
        var signal = new CompletableFuture<TxStreamFinishedResponse>();

        // when
        handler.onTxStreamFinishedResponse( signal, mock( TxStreamFinishedResponse.class ) );

        // then
        verify( catchupProcess ).streamComplete();
        assertTrue( signal.isDone() );
    }

    @Test
    void shouldSignalCompleteAndLogOnCatchupErrorResponse()
    {
        // given
        var logProvider = new AssertableLogProvider();
        var log = logProvider.getLog( CatchupPollingProcess.class );
        var catchupProcess = mock( CatchupPollingProcess.class );
        var handler = new TxPullResponseHandler( catchupProcess, log );
        var signal = new CompletableFuture<TxStreamFinishedResponse>();
        var errorMessage = "An error has occurred!";
        var error = new CatchupErrorResponse( CatchupResult.E_GENERAL_ERROR, errorMessage );

        // when
        handler.onCatchupErrorResponse( signal, error );

        // then
        assertThat( logProvider ).forClass( CatchupPollingProcess.class ).forLevel( WARN ).containsMessages( errorMessage );
        assertTrue( signal.isDone() );
    }
}
