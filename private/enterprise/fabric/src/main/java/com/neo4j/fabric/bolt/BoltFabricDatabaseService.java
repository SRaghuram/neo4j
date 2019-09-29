/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricException;
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
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.kernel.api.exceptions.Status.Statement.SemanticError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;

public class BoltFabricDatabaseService implements BoltGraphDatabaseServiceSPI
{

    private final FabricExecutor fabricExecutor;
    private final DatabaseId databaseId;
    private final FabricConfig config;
    private final TransactionManager transactionManager;

    public BoltFabricDatabaseService( DatabaseId databaseId,
            FabricExecutor fabricExecutor,
            FabricConfig config,
            TransactionManager transactionManager )
    {
        this.databaseId = databaseId;
        this.config = config;
        this.transactionManager = transactionManager;
        this.fabricExecutor = fabricExecutor;
    }

    @Override
    public BoltTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, Duration txTimeout,
            AccessMode accessMode, Map<String,Object> txMetadata )
    {
        // for transactions started by Bolt it holds that implicit transaction = periodic commit
        if ( type == KernelTransaction.Type.implicit )
        {
            throw new FabricException( SemanticError, "Periodic commit is not supported in Fabric" );
        }
        if ( txTimeout == null )
        {
            txTimeout = config.getTransactionTimeout();
        }

        FabricTransactionInfo transactionInfo = new FabricTransactionInfo(
                accessMode,
                loginContext,
                clientInfo,
                databaseId.name(),
                false,
                txTimeout,
                txMetadata
        );
        FabricTransaction fabricTransaction = transactionManager.begin( transactionInfo );
        return new BoltTransactionImpl( transactionInfo, fabricTransaction );
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return fabricExecutor.isPeriodicCommit( query );
    }

    @Override
    public void awaitUpToDate( List<Bookmark> bookmarks, Duration perBookmarkTimeout )
    {
        if ( bookmarks.isEmpty() )
        {
            return;
        }

        throw new FabricException( InvalidBookmark, "Bookmarks are currently not supported by Fabric" );
    }

    @Override
    public long newestEncounteredTxId()
    {
        return -1;
    }

    @Override
    public DatabaseId getDatabaseId()
    {
        return databaseId;
    }

    private class BoltTransactionImpl implements BoltTransaction
    {

        private final FabricTransactionInfo transactionInfo;
        private final FabricTransaction fabricTransaction;

        BoltTransactionImpl( FabricTransactionInfo transactionInfo, FabricTransaction fabricTransaction )
        {
            this.transactionInfo = transactionInfo;
            this.fabricTransaction = fabricTransaction;
        }

        @Override
        public void commit() throws TransactionFailureException
        {
            fabricTransaction.commit();
        }

        @Override
        public void rollback() throws TransactionFailureException
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
        public BoltQueryExecution executeQuery( String query, MapValue parameters, boolean prePopulate, QuerySubscriber subscriber )
                throws QueryExecutionKernelException
        {
            StatementResult statementResult = fabricExecutor.run( fabricTransaction, query, parameters );
            return new BoltQueryExecutionImpl( statementResult, subscriber, config );
        }
    }
}
