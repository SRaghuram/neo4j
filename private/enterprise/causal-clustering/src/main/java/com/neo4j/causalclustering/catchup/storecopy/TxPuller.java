/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpWriter;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.configuration.CausalClusteringSettings;

import java.net.ConnectException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.CappedLogger;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static java.lang.Long.max;
import static java.lang.String.format;

class TxPuller
{
    private final StateBasedAddressProvider addressProvider;
    private final Log log;
    private final ResettableCondition resettableCondition;
    private final CappedLogger connectionErrorLogger;
    private State currentState = State.NORMAL;
    private long highestTx;

    static TxPuller createTxPuller( CatchupAddressProvider catchupAddressProvider, LogProvider logProvider, Config config, NamedDatabaseId namedDatabaseId )
    {
        Clock clock = Clock.systemUTC();
        Duration inactivityTimeout = config.get( CausalClusteringSettings.catch_up_client_inactivity_timeout );
        return new TxPuller( catchupAddressProvider, logProvider, new ResettableTimeout( inactivityTimeout, clock ), clock, namedDatabaseId );
    }

    @VisibleForTesting
    TxPuller( CatchupAddressProvider catchupAddressProvider, LogProvider logProvider, ResettableCondition noProgressHandler, Clock clock,
            NamedDatabaseId namedDatabaseId )
    {
        this.addressProvider = new StateBasedAddressProvider( namedDatabaseId, catchupAddressProvider );
        this.log = logProvider.getLog( getClass() );
        this.resettableCondition = noProgressHandler;
        this.connectionErrorLogger = new CappedLogger( log );
        connectionErrorLogger.setTimeLimit( 1, TimeUnit.SECONDS, clock );
    }

    void pullTransactions( TxPullRequestContext context, TransactionLogCatchUpWriter writer, TxPullClient client ) throws StoreCopyFailedException
    {
        final StoreId expectedStoreId = context.expectedStoreId();

        long requestedTxId = context.startTxIdExclusive();
        do
        {
            Result result = execute( client, expectedStoreId, writer, requestedTxId );
            updateState( result, writer, context );
            requestedTxId = max( requestedTxId, fallbackOnLastAttempt( writer, context ) );
        }
        while ( shouldContinue() );

        // one last request towards primary (presumably most up to date instance). This request may fail since our constraint has been met.
        execute( client, expectedStoreId, writer, requestedTxId );
    }

    /**
     * For a particular scenario when seeding a cluster from a store with no tx logs. We will result to fallback id on last attempt if no progress has been made
     */
    private long fallbackOnLastAttempt( TransactionLogCatchUpWriter writer, TxPullRequestContext context )
    {
        long currentTxId = writer.lastTx();
        if ( currentState == State.LAST_ATTEMPT && currentTxId == -1 )
        {
            return context.fallbackStartId().orElse( currentTxId );
        }
        return currentTxId;
    }

    private Result execute( TxPullClient txPullClient, StoreId expectedStoreId, TransactionLogCatchUpWriter writer, long fromTxId )
    {
        try
        {
            SocketAddress fromAddress = addressProvider.get( currentState );
            try
            {
                log.info( "Pulling transactions from %s starting with txId: %d", fromAddress, fromTxId );
                CatchupResult status = txPullClient.pullTransactions( fromAddress, expectedStoreId, fromTxId, writer ).status();
                return setResult( status );
            }
            catch ( ConnectException e )
            {
                connectionErrorLogger.info( "Unable to connect. [Address: %s] [Message: %s]", fromAddress, e.getMessage() );
                return Result.TRANSIENT_ERROR;
            }
            catch ( Exception e )
            {
                log.warn( format( "Unexpected exception when pulling transactions. [Address: %s]", fromAddress ), e );
                return Result.ERROR;
            }
        }
        catch ( CatchupAddressResolutionException e )
        {
            log.info( "Unable to find a suitable address to pull transactions from [Message: %s]", e.getMessage());
            return Result.TRANSIENT_ERROR;
        }
    }

    private Result setResult( CatchupResult status )
    {
        if ( status == SUCCESS_END_OF_STREAM )
        {
            return Result.SUCCESS;
        }
        else
        {
            log.info( "Transaction pulling attempt failed with error: %s", status );
            return Result.ERROR;
        }
    }

    private boolean shouldContinue()
    {
        return currentState != State.COMPLETE;
    }

    private void updateState( Result result, TransactionLogCatchUpWriter writer, TxPullRequestContext context ) throws StoreCopyFailedException
    {
        long currentHighest = max( highestTx, writer.lastTx() );
        try
        {
            boolean completed = context.constraintReached( currentHighest ) && result == Result.SUCCESS;
            if ( completed )
            {
                currentState = State.COMPLETE;
                return;
            }
            currentState = checkProgress( currentHighest, result );
        }
        finally
        {
            highestTx = currentHighest;
        }
    }

    private State checkProgress( long lastWrittenTx, Result result ) throws StoreCopyFailedException
    {
        if ( hasProgressed( lastWrittenTx ) )
        {
            resettableCondition.reset();
            return State.NORMAL;
        }
        else
        {

            if ( currentState == State.LAST_ATTEMPT )
            {
                log.warn( "Failed to pull transactions" );
                throw new StoreCopyFailedException( "Pulling tx failed consecutively without progress" );
            }
            if ( resettableCondition.canContinue() && result != Result.ERROR )
            {
                return State.NORMAL;
            }
            else
            {
                return State.LAST_ATTEMPT;
            }
        }
    }

    private boolean hasProgressed( long lastWrittenTx )
    {
        return lastWrittenTx > highestTx;
    }

    private static class StateBasedAddressProvider
    {
        private final NamedDatabaseId namedDatabaseId;
        private final CatchupAddressProvider catchupAddressProvider;

        private StateBasedAddressProvider( NamedDatabaseId namedDatabaseId, CatchupAddressProvider catchupAddressProvider )
        {
            this.namedDatabaseId = namedDatabaseId;
            this.catchupAddressProvider = catchupAddressProvider;
        }

        public SocketAddress get( State state ) throws CatchupAddressResolutionException
        {
            switch ( state )
            {
            case LAST_ATTEMPT:
            case COMPLETE:
                return catchupAddressProvider.primary( namedDatabaseId );
            default:
                return catchupAddressProvider.secondary( namedDatabaseId );
            }
        }
    }

    private enum Result
    {
        SUCCESS,
        TRANSIENT_ERROR,
        ERROR
    }

    private enum State
    {
        COMPLETE,
        LAST_ATTEMPT,
        NORMAL
    }
}
