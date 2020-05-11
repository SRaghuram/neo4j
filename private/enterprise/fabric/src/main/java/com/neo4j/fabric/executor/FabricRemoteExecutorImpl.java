/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.SingleDbTransaction;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.CompositeTransaction;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.values.virtual.MapValue;

public class FabricRemoteExecutorImpl implements FabricRemoteExecutor
{
    private final DriverPool driverPool;

    public FabricRemoteExecutorImpl( DriverPool driverPool )
    {
        this.driverPool = driverPool;
    }

    public RemoteTransactionContext startTransactionContext( CompositeTransaction compositeTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager )
    {
        return new RemoteTransactionContextImpl( compositeTransaction, transactionInfo, bookmarkManager );
    }

    private class RemoteTransactionContextImpl implements RemoteTransactionContext
    {
        private final Map<Long,PooledDriver> usedDrivers = new ConcurrentHashMap<>();
        private final Map<Long,DriverTxWrapper> driverTransactions = new ConcurrentHashMap<>();

        private final CompositeTransaction compositeTransaction;
        private final FabricTransactionInfo transactionInfo;
        private final TransactionBookmarkManager bookmarkManager;

        private RemoteTransactionContextImpl( CompositeTransaction compositeTransaction, FabricTransactionInfo transactionInfo,
                TransactionBookmarkManager bookmarkManager )
        {
            this.compositeTransaction = compositeTransaction;
            this.transactionInfo = transactionInfo;
            this.bookmarkManager = bookmarkManager;
        }

        public Mono<StatementResult> run( Location.Remote location, String query, TransactionMode transactionMode, MapValue params )
        {
            var driverTx = getOrCreateTx( location, transactionMode );
            return runInTx( driverTx, query, params );
        }

        private Mono<FabricDriverTransaction> getOrCreateTx( Location.Remote location, TransactionMode transactionMode )
        {
            var existingTx = driverTransactions.get( location.getGraphId() );
            if ( existingTx != null )
            {
                maybeUpgradeToWritingTransaction( existingTx, transactionMode );
                return existingTx.driverTx;
            }

            return driverTransactions.computeIfAbsent( location.getGraphId(), locationId ->
            {
                switch ( transactionMode )
                {
                case DEFINITELY_WRITE:
                    return compositeTransaction.startWritingTransaction( location, () ->
                    {
                        var tx = beginDriverTx( location, AccessMode.WRITE );
                        return new DriverTxWrapper( tx, location, bookmarkManager );
                    } );

                case MAYBE_WRITE:
                    return compositeTransaction.startReadingTransaction( location, () ->
                    {
                        var tx = beginDriverTx( location, AccessMode.WRITE );
                        return new DriverTxWrapper( tx, location, bookmarkManager );
                    } );

                case DEFINITELY_READ:
                    return compositeTransaction.startReadingOnlyTransaction( location, () ->
                    {
                        var tx = beginDriverTx( location, AccessMode.READ );
                        return new DriverTxWrapper( tx, location, bookmarkManager );
                    } );
                default:
                    throw new IllegalArgumentException( "Unexpected transaction mode: " + transactionMode );
                }
            } ).driverTx;
        }

        @Override
        public void close()
        {
            usedDrivers.values().forEach( PooledDriver::release );
        }

        private void maybeUpgradeToWritingTransaction( DriverTxWrapper tx, TransactionMode transactionMode )
        {
            if ( transactionMode == TransactionMode.DEFINITELY_WRITE )
            {
                compositeTransaction.upgradeToWritingTransaction( tx );
            }
        }

        private Mono<FabricDriverTransaction> beginDriverTx( Location.Remote location, AccessMode accessMode )
        {
            var driver = getDriver( location );
            var bookmarks = bookmarkManager.getBookmarksForRemote( location );
            return driver.beginTransaction( location, accessMode, transactionInfo, bookmarks );
        }

        private PooledDriver getDriver( Location.Remote location )
        {
            return usedDrivers.computeIfAbsent( location.getGraphId(), gid -> driverPool.getDriver( location, transactionInfo.getLoginContext().subject() ) );
        }

        private Mono<StatementResult> runInTx( Mono<FabricDriverTransaction> tx, String query, MapValue params )
        {
            return tx.map( rxTransaction -> rxTransaction.run( query, params ) ).cache();
        }
    }

    private static class DriverTxWrapper implements SingleDbTransaction
    {
        private final Mono<FabricDriverTransaction> driverTx;
        private final Location.Remote location;
        private final TransactionBookmarkManager bookmarkManager;

        DriverTxWrapper( Mono<FabricDriverTransaction> driverTx, Location.Remote location, TransactionBookmarkManager bookmarkManager )
        {
            this.driverTx = driverTx;
            this.location = location;
            this.bookmarkManager = bookmarkManager;
        }

        @Override
        public Mono<Void> commit()
        {
            return driverTx.flatMap( FabricDriverTransaction::commit )
                    .doOnSuccess( bookmark -> bookmarkManager.remoteTransactionCommitted( location, bookmark ) )
                    .then();
        }

        @Override
        public Mono<Void> rollback()
        {
            return driverTx.flatMap( FabricDriverTransaction::rollback ).then();
        }

        @Override
        public Mono<Void> terminate()
        {
            // driver does not have 'terminate' operation
            return rollback();
        }

        @Override
        public Location getLocation()
        {
            return location;
        }
    }
}
