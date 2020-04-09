/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import com.neo4j.fabric.bookmark.TransactionBookmarkManagerFactory;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricExecutor;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.transaction.FabricTransaction;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import com.neo4j.fabric.transaction.TransactionManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

public class BoltFabricDatabaseService implements BoltGraphDatabaseServiceSPI
{

    private final FabricExecutor fabricExecutor;
    private final NamedDatabaseId namedDatabaseId;
    private final FabricConfig config;
    private final TransactionManager transactionManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final TransactionBookmarkManagerFactory transactionBookmarkManagerFactory;

    public BoltFabricDatabaseService( NamedDatabaseId namedDatabaseId,
            FabricExecutor fabricExecutor,
            FabricConfig config,
            TransactionManager transactionManager,
            LocalGraphTransactionIdTracker transactionIdTracker,
            TransactionBookmarkManagerFactory transactionBookmarkManagerFactory )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricExecutor = fabricExecutor;
        this.transactionIdTracker = transactionIdTracker;
        this.transactionBookmarkManagerFactory = transactionBookmarkManagerFactory;
    }

    @Override
    public BoltTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, List<Bookmark> bookmarks,
            Duration txTimeout, AccessMode accessMode, Map<String,Object> txMetadata )
    {
        if ( txTimeout == null )
        {
            txTimeout = config.getTransactionTimeout();
        }

        FabricTransactionInfo transactionInfo = new FabricTransactionInfo(
                accessMode,
                loginContext,
                clientInfo,
                namedDatabaseId.name(),
                false,
                txTimeout,
                txMetadata
        );

        var transactionBookmarkManager = transactionBookmarkManagerFactory.createTransactionBookmarkManager( transactionIdTracker );
        transactionBookmarkManager.processSubmittedByClient( bookmarks );

        FabricTransaction fabricTransaction = transactionManager.begin( transactionInfo, transactionBookmarkManager );
        return new BoltTransactionImpl( transactionInfo, fabricTransaction );
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return fabricExecutor.isPeriodicCommit( query );
    }

    @Override
    public NamedDatabaseId getNamedDatabaseId()
    {
        return namedDatabaseId;
    }

    private class BoltTransactionImpl implements BoltTransaction
    {
        private final FabricTransaction fabricTransaction;

        BoltTransactionImpl( FabricTransactionInfo transactionInfo, FabricTransaction fabricTransaction )
        {
            this.fabricTransaction = fabricTransaction;
        }

        @Override
        public void commit()
        {
            fabricTransaction.commit();
        }

        @Override
        public void rollback()
        {
            fabricTransaction.rollback();
        }

        @Override
        public void markForTermination( Status reason )
        {
            fabricTransaction.markForTermination( reason );
        }

        @Override
        public void markForTermination()
        {
            fabricTransaction.markForTermination( Terminated );
        }

        @Override
        public Optional<Status> getReasonIfTerminated()
        {
            return fabricTransaction.getReasonIfTerminated();
        }

        @Override
        public BookmarkMetadata getBookmarkMetadata()
        {
            return fabricTransaction.getBookmarkManager().constructFinalBookmark();
        }

        @Override
        public BoltQueryExecution executeQuery( String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber )
        {
            StatementResult statementResult = fabricExecutor.run( fabricTransaction, query, parameters );
            return new BoltQueryExecutionImpl( statementResult, subscriber, config );
        }
    }
}
