/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.bookmark.TransactionBookmarkManager;
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
import org.neo4j.internal.kernel.api.security.SecurityContext;
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

    public LocalTransactionContext startTransactionContext( CompositeTransaction compositeTransaction,
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager )
    {
        return new LocalTransactionContext( compositeTransaction, transactionInfo, bookmarkManager );
    }

    public class LocalTransactionContext implements AutoCloseable
    {
        private final Map<Long, KernelTxWrapper> kernelTransactions = new ConcurrentHashMap<>();
        private final Set<InternalTransaction> internalTransactions = ConcurrentHashMap.newKeySet();

        private final CompositeTransaction compositeTransaction;
        private final FabricTransactionInfo transactionInfo;
        private final TransactionBookmarkManager bookmarkManager;

        private LocalTransactionContext( CompositeTransaction compositeTransaction,
                FabricTransactionInfo transactionInfo,
                TransactionBookmarkManager bookmarkManager )
        {
            this.compositeTransaction = compositeTransaction;
            this.transactionInfo = transactionInfo;
            this.bookmarkManager = bookmarkManager;
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

        public Set<InternalTransaction> getInternalTransactions()
        {
            return internalTransactions;
        }

        public FabricKernelTransaction getOrCreateTx( Location.Local location, TransactionMode transactionMode, ExecutingQuery parentQuery )
        {
            var existingTx = kernelTransactions.get( location.getGraphId() );
            if ( existingTx != null )
            {
                maybeUpgradeToWritingTransaction( existingTx, transactionMode );
                return existingTx.fabricKernelTransaction;
            }

            // it is important to try to get the facade before handling bookmarks
            // Unlike the bookmark logic, this will fail gracefully if the database is not available
            var databaseFacade = getDatabaseFacade( location );

            bookmarkManager.awaitUpToDate( location );
            return kernelTransactions.computeIfAbsent( location.getGraphId(), locationId ->
            {
                switch ( transactionMode )
                {
                case DEFINITELY_WRITE:
                    return compositeTransaction.startWritingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.WRITE, parentQuery );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
                    } );

                case MAYBE_WRITE:
                    return compositeTransaction.startReadingTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.WRITE, parentQuery );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
                    } );

                case DEFINITELY_READ:
                    return compositeTransaction.startReadingOnlyTransaction( location, () ->
                    {
                        var tx = beginKernelTx( databaseFacade, AccessMode.READ, parentQuery );
                        return new KernelTxWrapper( tx, bookmarkManager, location );
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

        private FabricKernelTransaction beginKernelTx( GraphDatabaseFacade databaseFacade, AccessMode accessMode, ExecutingQuery parentQuery )
        {
            var dependencyResolver = databaseFacade.getDependencyResolver();
            var executionEngine = dependencyResolver.resolveDependency( ExecutionEngine.class );

            var internalTransaction = beginInternalTransaction( databaseFacade, transactionInfo );

            var queryService = dependencyResolver.resolveDependency( GraphDatabaseQueryService.class );
            var transactionalContextFactory = Neo4jTransactionalContextFactory.create( queryService );

            return new FabricKernelTransaction( parentQuery,executionEngine, transactionalContextFactory, internalTransaction, config );
        }

        private GraphDatabaseFacade getDatabaseFacade( Location.Local location )
        {
            try
            {
                return dbms.getDatabase( location.getDatabaseName() );
            }
            catch ( UnavailableException e )
            {
                throw new FabricException( Status.Database.DatabaseUnavailable, e );
            }
        }

        private InternalTransaction beginInternalTransaction( GraphDatabaseFacade databaseFacade, FabricTransactionInfo transactionInfo )
        {
            InternalTransaction internalTransaction;
            KernelTransaction.Type kernelTransactionType = getKernelTransactionType( transactionInfo );
            var loginContext = maybeRestrictLoginContext( transactionInfo.getLoginContext(), databaseFacade.databaseName() );

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

            internalTransactions.add( internalTransaction );

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

        private LoginContext maybeRestrictLoginContext( LoginContext originalContext, String databaseName )
        {
            boolean isConfiguredFabricDatabase = dbms.isFabricDatabase( databaseName );
            return isConfiguredFabricDatabase
                   ? wrappedLoginContext( originalContext )
                   : transactionInfo.getLoginContext();
        }

        private LoginContext wrappedLoginContext( LoginContext originalContext )
        {
            if ( originalContext instanceof EnterpriseLoginContext )
            {
                return new FabricLocalEnterpriseLoginContext( (EnterpriseLoginContext) originalContext );
            }
            else
            {
                return new FabricLocalLoginContext( originalContext );
            }
        }
    }

    private static class KernelTxWrapper implements SingleDbTransaction
    {

        private final FabricKernelTransaction fabricKernelTransaction;
        private final TransactionBookmarkManager bookmarkManager;
        private final Location.Local location;

        KernelTxWrapper( FabricKernelTransaction fabricKernelTransaction, TransactionBookmarkManager bookmarkManager, Location.Local location )
        {
            this.fabricKernelTransaction = fabricKernelTransaction;
            this.bookmarkManager = bookmarkManager;
            this.location = location;
        }

        @Override
        public Mono<Void> commit()
        {
            fabricKernelTransaction.commit();
            bookmarkManager.localTransactionCommitted( location );
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

    private static class FabricLocalEnterpriseLoginContext implements EnterpriseLoginContext
    {
        private final EnterpriseLoginContext inner;

        private FabricLocalEnterpriseLoginContext( EnterpriseLoginContext inner )
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

    private static class FabricLocalLoginContext implements LoginContext
    {
        private final LoginContext inner;

        private FabricLocalLoginContext( LoginContext inner )
        {
            this.inner = inner;
        }

        @Override
        public AuthSubject subject()
        {
            return inner.subject();
        }

        @Override
        public SecurityContext authorize( IdLookup idLookup, String dbName )
        {
            var originalSecurityContext = inner.authorize( idLookup, dbName );
            var restrictedAccessMode =
                    new RestrictedAccessMode( originalSecurityContext.mode(), org.neo4j.internal.kernel.api.security.AccessMode.Static.ACCESS );
            return new SecurityContext( inner.subject(), restrictedAccessMode );
        }
    }
}
