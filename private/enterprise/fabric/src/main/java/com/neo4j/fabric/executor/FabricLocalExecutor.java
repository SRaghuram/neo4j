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
import com.neo4j.fabric.transaction.CompositeTransaction;
import com.neo4j.fabric.transaction.FabricTransactionInfo;
import com.neo4j.fabric.transaction.TransactionMode;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
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

    public LocalTransactionContext startTransactionContext( CompositeTransaction compositeTransaction, FabricTransactionInfo transactionInfo )
    {
        return new LocalTransactionContext( compositeTransaction, transactionInfo );
    }

    public class LocalTransactionContext implements AutoCloseable
    {
        private final Map<Long, KernelTxWrapper> kernelTransactions = new ConcurrentHashMap<>();

        private final CompositeTransaction compositeTransaction;
        private final FabricTransactionInfo transactionInfo;

        private LocalTransactionContext( CompositeTransaction compositeTransaction, FabricTransactionInfo transactionInfo )
        {
            this.compositeTransaction = compositeTransaction;
            this.transactionInfo = transactionInfo;
        }

        public StatementResult run( Location.Local location, TransactionMode transactionMode, ExecutingQuery parentQuery, FullyParsedQuery query,
                MapValue params, Flux<Record> input )
        {
            var kernelTransaction = getOrCreateTx( location, transactionMode, parentQuery );
            return kernelTransaction.run( query, params, input );
        }

        @Override
        public void close()
        {

        }

        private FabricKernelTransaction getOrCreateTx( Location.Local location, TransactionMode transactionMode, ExecutingQuery parentQuery )
        {
            var existingTx = kernelTransactions.get( location.getId() );
            if ( existingTx != null )
            {
                maybeUpgradeToWritingTransaction( existingTx, transactionMode );
                return existingTx.fabricKernelTransaction;
            }

            return kernelTransactions.computeIfAbsent( location.getId(), locationId ->
            {
                switch ( transactionMode )
                {
                case DEFINITELY_WRITE:
                    return compositeTransaction.startWritingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( location, AccessMode.WRITE, parentQuery );
                        return new KernelTxWrapper( tx, location );
                    } );

                case MAYBE_WRITE:
                    return compositeTransaction.startReadingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( location, AccessMode.WRITE, parentQuery );
                        return new KernelTxWrapper( tx, location );
                    } );

                case DEFINITELY_READ:
                    return compositeTransaction.startReadingOnlyTransaction( location, () ->
                    {
                        var tx = beginKernelTx( location, AccessMode.READ, parentQuery );
                        return new KernelTxWrapper( tx, location );
                    } );
                default:
                    throw new IllegalArgumentException( "Unexpected transaction mode: " + transactionMode );
                }
            } ).fabricKernelTransaction;
        }

        private void maybeUpgradeToWritingTransaction( KernelTxWrapper tx, TransactionMode transactionMode )
        {
            if ( transactionMode == TransactionMode.DEFINITELY_WRITE )
            {
                compositeTransaction.upgradeToWritingTransaction( tx );
            }
        }

        private FabricKernelTransaction beginKernelTx( Location.Local location, AccessMode accessMode, ExecutingQuery parentQuery )
        {
            GraphDatabaseFacade databaseFacade;
            try
            {
                databaseFacade = dbms.getDatabase( location.getDatabaseName() );
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

            return new FabricKernelTransaction( parentQuery,executionEngine, transactionalContextFactory, internalTransaction, config );
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

        private KernelTransaction.Type getKernelTransactionType( FabricTransactionInfo fabricTransactionInfo )
        {
            if ( fabricTransactionInfo.isImplicitTransaction() )
            {
                return KernelTransaction.Type.IMPLICIT;
            }

            return KernelTransaction.Type.EXPLICIT;
        }

        private LoginContext maybeRestrictLoginContext( EnterpriseLoginContext originalContext, String databaseName )
        {
            boolean isConfiguredFabricDatabase = dbms.isConfiguredFabricDatabase( databaseName );
            return isConfiguredFabricDatabase
                   ? new FabricLocalLoginContext( originalContext )
                   : transactionInfo.getLoginContext();
        }
    }

    private class KernelTxWrapper implements SingleDbTransaction
    {

        private final FabricKernelTransaction fabricKernelTransaction;
        private final Location location;

        KernelTxWrapper( FabricKernelTransaction fabricKernelTransaction, Location location )
        {
            this.fabricKernelTransaction = fabricKernelTransaction;
            this.location = location;
        }

        @Override
        public Mono<Void> commit()
        {
            fabricKernelTransaction.commit();
            return Mono.empty();
        }

        @Override
        public Mono<Void> rollback()
        {
            fabricKernelTransaction.rollback();
            return Mono.empty();
        }

        @Override
        public Location getLocation()
        {
            return location;
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
            var restrictedAccessMode =
                    new RestrictedAccessMode( originalSecurityContext.mode(), org.neo4j.internal.kernel.api.security.AccessMode.Static.ACCESS );
            return new EnterpriseSecurityContext( inner.subject(), restrictedAccessMode, inner.roles(), action -> false );
        }
    }
}
