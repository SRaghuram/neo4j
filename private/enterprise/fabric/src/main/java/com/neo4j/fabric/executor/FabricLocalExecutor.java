/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.stream.InputDataStreamImpl;
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.values.virtual.MapValue;

public class FabricLocalExecutor
{
    private final FabricConfig config;
    private final FabricDatabaseManager dbms;

    public FabricLocalExecutor( FabricConfig config, FabricDatabaseManager dbms )
    {
        this.config = config;
        this.dbms = dbms;
    }

    public FabricLocalTransaction begin( FabricTransactionInfo transactionInfo )
    {
        String databaseName = transactionInfo.getDatabaseName();
        GraphDatabaseFacade databaseFacade;
        try
        {
            databaseFacade = dbms.getDatabase( databaseName );
        }
        catch ( UnavailableException e )
        {
            throw new FabricException( Status.General.DatabaseUnavailable, e );
        }

        var dependencyResolver = databaseFacade.getDependencyResolver();
        var executionEngine = dependencyResolver.resolveDependency( ExecutionEngine.class );
        var txBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );

        var internalTransaction = beginInternalTransaction( databaseFacade, transactionInfo );
        var kernelTransaction = txBridge.getKernelTransactionBoundToThisThread( false, databaseFacade.databaseId() );

        var queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
        var transactionalContextFactory = Neo4jTransactionalContextFactory.create( queryService );

        return new FabricLocalTransaction( txBridge, executionEngine, transactionalContextFactory, kernelTransaction, internalTransaction );
    }

    private InternalTransaction beginInternalTransaction( GraphDatabaseFacade databaseFacade, FabricTransactionInfo transactionInfo )
    {
        InternalTransaction internalTransaction;
        Transaction.Type kernelTransactionType = getKernelTransactionType( transactionInfo );
        FabricLocalLoginContext loginContext = new FabricLocalLoginContext( (EnterpriseLoginContext) transactionInfo.getLoginContext() );
        if ( transactionInfo.getTxTimeout() == null )
        {
            internalTransaction = databaseFacade.beginTransaction( kernelTransactionType, loginContext, transactionInfo.getClientConnectionInfo() );
        }
        else
        {
            internalTransaction = databaseFacade.beginTransaction( kernelTransactionType, loginContext, transactionInfo.getClientConnectionInfo(),
                    transactionInfo.getTxTimeout().toMillis(), TimeUnit.MILLISECONDS );
        }

        if ( transactionInfo.getTxMetadata() != null )
        {
            internalTransaction.setMetaData( transactionInfo.getTxMetadata() );
        }

        return internalTransaction;
    }

    private KernelTransaction.Type getKernelTransactionType( FabricTransactionInfo fabricTransactionInfo )
    {
        if ( fabricTransactionInfo.isImplicitTransaction() )
        {
            return KernelTransaction.Type.implicit;
        }

        return KernelTransaction.Type.explicit;
    }

    public class FabricLocalTransaction
    {
        private final ExecutionEngine queryExecutionEngine;
        private final TransactionalContextFactory transactionalContextFactory;
        private final KernelTransaction kernelTransaction;
        private final InternalTransaction internalTransaction;
        private final ThreadToStatementContextBridge txBridge;

        FabricLocalTransaction( ThreadToStatementContextBridge txBridge, ExecutionEngine queryExecutionEngine,
                TransactionalContextFactory transactionalContextFactory, KernelTransaction kernelTransaction,
                InternalTransaction internalTransaction )
        {
            this.txBridge = txBridge;
            this.queryExecutionEngine = queryExecutionEngine;
            this.transactionalContextFactory = transactionalContextFactory;
            this.kernelTransaction = kernelTransaction;
            this.internalTransaction = internalTransaction;
        }

        public StatementResult run( FullyParsedQuery query, MapValue params, StatementResult input )
        {

            return StatementResults.create( subscriber -> execute( query, params, convert( input ), subscriber ) );
        }

        private QueryExecution execute( FullyParsedQuery query, MapValue params, InputDataStream input, QuerySubscriber subscriber )
        {
            try
            {
                var currentExecutionContext = transactionalContextFactory.newContext( internalTransaction, "", params );
                return queryExecutionEngine.executeQuery( query, params, currentExecutionContext, true, input, subscriber );
            }
            catch ( Exception e )
            {
                throw new FabricException( Status.Statement.ExecutionFailed, e );
            }
        }

        private InputDataStream convert( StatementResult input )
        {
            return new InputDataStreamImpl(
                    new Rx2SyncStream(
                            input,
                            config.getDataStream().getBufferLowWatermark(),
                            config.getDataStream().getBufferSize(),
                            config.getDataStream().getSyncBatchSize() )
            );
        }

        public void commit() throws TransactionFailureException
        {
            if ( kernelTransaction.isOpen() )
            {
                kernelTransaction.commit();
            }
        }

        public void rollback() throws TransactionFailureException
        {
            if ( kernelTransaction.isOpen() )
            {
                kernelTransaction.rollback();
            }
        }

        public void bindToCurrentThread()
        {
            txBridge.bindTransactionToCurrentThread( kernelTransaction );
        }

        public void unbindFromCurrentThread()
        {
            txBridge.unbindTransactionFromCurrentThread();
        }

        public void markForTermination( Status reason )
        {
            kernelTransaction.markForTermination( reason );
        }
    }

    private static class FabricLocalLoginContext implements EnterpriseLoginContext
    {
        private final EnterpriseLoginContext inner;

        private FabricLocalLoginContext( EnterpriseLoginContext inner )
        {
            this.inner = inner;
        }

        @Override
        public AuthSubject subject()
        {
            return inner.subject();
        }

        @Override
        public Set<String> roles()
        {
            return inner.roles();
        }

        @Override
        public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName ) throws KernelException
        {
            var originalSecurityContext = inner.authorize( idLookup, dbName );
            var restrictedAccessMode = new RestrictedAccessMode( originalSecurityContext.mode(), AccessMode.Static.READ );
            return new EnterpriseSecurityContext( inner.subject(), restrictedAccessMode, inner.roles(), originalSecurityContext.isAdmin() );
        }
    }
}
