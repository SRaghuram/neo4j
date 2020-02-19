/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.localdb.FabricDatabaseManager;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
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
        return new FabricLocalTransaction( transactionInfo );
    }

    private KernelTransaction.Type getKernelTransactionType( FabricTransactionInfo fabricTransactionInfo )
    {
        if ( fabricTransactionInfo.isImplicitTransaction() )
        {
            return KernelTransaction.Type.IMPLICIT;
        }

        return KernelTransaction.Type.EXPLICIT;
    }

    public class FabricLocalTransaction
    {
        private final FabricTransactionInfo transactionInfo;
        private final Set<SingleStatementKernelTransaction> kernelTransactions = Collections.newSetFromMap( new ConcurrentHashMap<>() );

        FabricLocalTransaction( FabricTransactionInfo transactionInfo )
        {
            this.transactionInfo = transactionInfo;
        }

        public StatementResult run( ExecutingQuery parentQuery, FullyParsedQuery query, MapValue params, Flux<Record> input )
        {
            var kernelTransaction = beginKernelTransaction( parentQuery );
            kernelTransactions.add( kernelTransaction );

            try
            {
                var result = kernelTransaction.run( query, params, input );

                return new ResultInterceptor( result, () ->
                {
                    kernelTransaction.commit();
                    kernelTransactions.remove( kernelTransaction );
                }, () ->
                {
                    kernelTransaction.rollback();
                    kernelTransactions.remove( kernelTransaction );
                } );
            }
            catch ( RuntimeException e )
            {
                kernelTransaction.rollback();
                kernelTransactions.remove( kernelTransaction );
                throw e;
            }
        }

        public void commit()
        {
            kernelTransactions.forEach( SingleStatementKernelTransaction::commit );
        }

        public void rollback()
        {
            kernelTransactions.forEach( SingleStatementKernelTransaction::rollback );
        }

        public void terminate()
        {
            kernelTransactions.forEach( SingleStatementKernelTransaction::terminate );
        }

        private SingleStatementKernelTransaction beginKernelTransaction( ExecutingQuery parentQuery )
        {
            String databaseName = transactionInfo.getDatabaseName();
            GraphDatabaseFacade databaseFacade;
            try
            {
                databaseFacade = dbms.getDatabase( databaseName );
            }
            catch ( UnavailableException e )
            {
                throw new FabricException( Status.Database.DatabaseUnavailable, e );
            }

            var dependencyResolver = databaseFacade.getDependencyResolver();
            var executionEngine = dependencyResolver.resolveDependency( ExecutionEngine.class );

            var internalTransaction = beginInternalTransaction( databaseFacade, transactionInfo );

            var queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
            var transactionalContextFactory = Neo4jTransactionalContextFactory.create( queryService );

            return new SingleStatementKernelTransaction( parentQuery,executionEngine, transactionalContextFactory, internalTransaction, config );
        }

        private InternalTransaction beginInternalTransaction( GraphDatabaseFacade databaseFacade, FabricTransactionInfo transactionInfo )
        {
            InternalTransaction internalTransaction;
            KernelTransaction.Type kernelTransactionType = getKernelTransactionType( transactionInfo );
            var loginContext = maybeRestrictLoginContext( (EnterpriseLoginContext) transactionInfo.getLoginContext(), databaseFacade.databaseName() );

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

        private LoginContext maybeRestrictLoginContext( EnterpriseLoginContext originalContext, String databaseName )
        {
            boolean isConfiguredFabricDatabase = dbms.isConfiguredFabricDatabase( databaseName );
            return isConfiguredFabricDatabase
                   ? new FabricLocalLoginContext( originalContext )
                   : transactionInfo.getLoginContext();
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
        public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
        {
            var originalSecurityContext = inner.authorize( idLookup, dbName );
            var restrictedAccessMode = new RestrictedAccessMode( originalSecurityContext.mode(), AccessMode.Static.ACCESS );
            return new EnterpriseSecurityContext( inner.subject(), restrictedAccessMode, inner.roles(), action -> false );
        }
    }

    private static class ResultInterceptor implements StatementResult
    {
        private final StatementResult wrappedResult;
        private final Runnable commit;
        private final Runnable rollback;

        ResultInterceptor( StatementResult wrappedResult, Runnable commit, Runnable rollback )
        {
            this.wrappedResult = wrappedResult;
            this.commit = commit;
            this.rollback = rollback;
        }

        @Override
        public Flux<String> columns()
        {
            return wrappedResult.columns().doOnError( error -> rollback.run() );
        }

        @Override
        public Flux<Record> records()
        {
            return wrappedResult.records().doOnError( error -> rollback.run() ).doOnComplete( commit ).doOnCancel( rollback );
        }

        @Override
        public Mono<Summary> summary()
        {
            return wrappedResult.summary();
        }
    }
}
