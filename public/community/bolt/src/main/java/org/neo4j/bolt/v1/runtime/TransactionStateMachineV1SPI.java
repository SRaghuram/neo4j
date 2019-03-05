/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

public class TransactionStateMachineV1SPI implements TransactionStateMachineSPI
{
    private final ThreadToStatementContextBridge txBridge;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionIdTracker transactionIdTracker;
    private final TransactionalContextFactory contextFactory;
    private final Duration txAwaitDuration;
    private final Clock clock;
    private final GraphDatabaseFacade databaseFacade;
    private final BoltChannel boltChannel;

    public TransactionStateMachineV1SPI( DatabaseContext databaseContext, BoltChannel boltChannel, Duration txAwaitDuration, Clock clock )
    {
        this.txBridge = resolveDependency( databaseContext, ThreadToStatementContextBridge.class );
        this.queryExecutionEngine = resolveDependency( databaseContext, QueryExecutionEngine.class );
        this.transactionIdTracker = newTransactionIdTracker( databaseContext );
        this.contextFactory = newTransactionalContextFactory( databaseContext );
        this.databaseFacade = databaseContext.getDatabaseFacade();
        this.boltChannel = boltChannel;
        this.txAwaitDuration = txAwaitDuration;
        this.clock = clock;
    }

    @Override
    public void awaitUpToDate( long oldestAcceptableTxId ) throws TransactionFailureException
    {
        transactionIdTracker.awaitUpToDate( oldestAcceptableTxId, txAwaitDuration );
    }

    @Override
    public long newestEncounteredTxId()
    {
        return transactionIdTracker.newestEncounteredTxId();
    }

    @Override
    public KernelTransaction beginTransaction( LoginContext loginContext, Duration txTimeout, Map<String,Object> txMetadata )
    {
        beginTransaction( explicit, loginContext, boltChannel.info(), txTimeout, txMetadata );
        return txBridge.getKernelTransactionBoundToThisThread( false );
    }

    @Override
    public void bindTransactionToCurrentThread( KernelTransaction tx )
    {
        txBridge.bindTransactionToCurrentThread( tx );
    }

    @Override
    public void unbindTransactionFromCurrentThread()
    {
        txBridge.unbindTransactionFromCurrentThread();
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return queryExecutionEngine.isPeriodicCommit( query );
    }

    @Override
    public BoltResultHandle executeQuery( LoginContext loginContext, String statement, MapValue params, Duration txTimeout,
            Map<String,Object> txMetadata )
    {
        InternalTransaction internalTransaction = beginTransaction( implicit, loginContext, boltChannel.info(), txTimeout, txMetadata );
        TransactionalContext transactionalContext = contextFactory.newContext( internalTransaction, statement, params );
        return newBoltResultHandle( statement, params, transactionalContext );
    }

    @Override
    public boolean supportsNestedStatementsInTransaction()
    {
        return false;
    }

    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, TransactionalContext transactionalContext )
    {
        return new BoltResultHandleV1( statement, params, transactionalContext );
    }

    private InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo, Duration txTimeout,
            Map<String,Object> txMetadata )
    {
        InternalTransaction tx;
        if ( txTimeout == null )
        {
            tx = databaseFacade.beginTransaction( type, loginContext, clientInfo );
        }
        else
        {
            tx = databaseFacade.beginTransaction( type, loginContext, clientInfo, txTimeout.toMillis(), TimeUnit.MILLISECONDS );
        }

        if ( txMetadata != null )
        {
            tx.setMetaData( txMetadata );
        }
        return tx;
    }

    private static TransactionIdTracker newTransactionIdTracker( DatabaseContext databaseContext )
    {
        Supplier<TransactionIdStore> transactionIdStoreSupplier = databaseContext.getDependencies().provideDependency( TransactionIdStore.class );
        AvailabilityGuard guard = resolveDependency( databaseContext, DatabaseAvailabilityGuard.class );
        return new TransactionIdTracker( transactionIdStoreSupplier, guard );
    }

    private static TransactionalContextFactory newTransactionalContextFactory( DatabaseContext databaseContext )
    {
        GraphDatabaseQueryService queryService = resolveDependency( databaseContext, GraphDatabaseQueryService.class );
        return Neo4jTransactionalContextFactory.create( queryService );
    }

    private static <T> T resolveDependency( DatabaseContext databaseContext, Class<T> clazz )
    {
        return databaseContext.getDependencies().resolveDependency( clazz );
    }

    public class BoltResultHandleV1 implements BoltResultHandle
    {
        private final String statement;
        private final MapValue params;
        private final TransactionalContext transactionalContext;

        public BoltResultHandleV1( String statement, MapValue params, TransactionalContext transactionalContext )
        {
            this.statement = statement;
            this.params = params;
            this.transactionalContext = transactionalContext;
        }

        @Override
        public BoltResult start() throws KernelException
        {
            try
            {
                BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
                QueryExecution result = queryExecutionEngine.executeQuery( statement, params, transactionalContext, true,
                        subscriber );
                return newBoltResult( result, subscriber, clock );

            }
            catch ( KernelException e )
            {
                close( false );
                throw new QueryExecutionKernelException( e );
            }
            catch ( Throwable e )
            {
                close( false );
                throw e;
            }
        }

        protected BoltResult newBoltResult( QueryExecution result,
                BoltAdapterSubscriber subscriber, Clock clock )
        {
            return new CypherAdapterStream( result, subscriber, clock );
        }

        @Override
        public void close( boolean success )
        {
            transactionalContext.close( success );
        }

        @Override
        public void terminate()
        {
            transactionalContext.terminate();
        }
    }

    public static class BoltAdapterSubscriber implements QuerySubscriber
    {
        private ThrowingConsumer<AnyValue[],IOException> recordConsumer;
        private Throwable error;
        private QueryStatistics statistics;
        private AnyValue[] values;

        @Override
        public void onResult( int numberOfFields )
        {
            this.values = new AnyValue[numberOfFields];
        }

        @Override
        public void onRecord()
        {
            //do nothing
        }

        @Override
        public void onField( int offset, AnyValue value )
        {
           values[offset] = value;
        }

        @Override
        public void onRecordCompleted() throws Exception
        {
            recordConsumer.accept(values);
        }

        @Override
        public void onError( Throwable throwable )
        {
            this.error = throwable;
        }

        @Override
        public void onResultCompleted( QueryStatistics statistics )
        {
            this.statistics = statistics;
        }

        QueryStatistics queryStatistics()
        {
            return statistics;
        }

        public void setRecordConsumer( ThrowingConsumer<AnyValue[],IOException> recordConsumer )
        {
            this.recordConsumer = recordConsumer;
        }
    }
}
