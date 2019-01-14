/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import java.util.Map;

import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Switches the transactional context of the thread while operating on the system graph.
 */
class ContextSwitchingSystemGraphQueryExecutor implements QueryExecutor
{
    private final DatabaseManager databaseManager;
    private final String activeDbName;
    private GraphDatabaseFacade systemDb;
    private ThreadToStatementContextBridge threadToStatementContextBridge;

    ContextSwitchingSystemGraphQueryExecutor( DatabaseManager databaseManager, String activeDbName )
    {
        this.databaseManager = databaseManager;
        this.activeDbName = activeDbName;
    }

    @Override
    public void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor )
    {
        final ThreadToStatementContextBridge statementContext = getThreadToStatementContextBridge();

        // pause outer transaction if there is one
        if ( statementContext.hasTransaction() )
        {
            final KernelTransaction outerTx = statementContext.getKernelTransactionBoundToThisThread( true );

            statementContext.unbindTransactionFromCurrentThread();

            try
            {
                systemDbExecute( query, params, resultVisitor );
            }
            finally
            {
                statementContext.unbindTransactionFromCurrentThread();
                statementContext.bindTransactionToCurrentThread( outerTx );
            }
        }
        else
        {
            systemDbExecute( query, params, resultVisitor );
        }
    }

    private void systemDbExecute( String query, Map<String,Object> parameters, QueryResult.QueryResultVisitor resultVisitor )
    {
        // NOTE: This transaction is executed with AUTH_DISABLED.
        // We need to make sure this method is only accessible from a SecurityContext with admin rights.
        try ( Transaction transaction = getSystemDb().beginTx() )
        {
            systemDbExecuteWithinTransaction( query, parameters, resultVisitor );
            transaction.success();
        }
    }

    private void systemDbExecuteWithinTransaction( String query, Map<String,Object> parameters,
            QueryResult.QueryResultVisitor resultVisitor )
    {
        Result result = getSystemDb().execute( query, parameters );
        QueryResult queryResult = ((QueryResultProvider) result).queryResult();
        //noinspection unchecked
        queryResult.accept( resultVisitor );
    }

    @Override
    public Transaction beginTx()
    {
        final ThreadToStatementContextBridge statementContext = getThreadToStatementContextBridge();
        final Runnable onClose;

        // pause outer transaction if there is one
        if ( statementContext.hasTransaction() )
        {
            final KernelTransaction outerTx = statementContext.getKernelTransactionBoundToThisThread( true );
            statementContext.unbindTransactionFromCurrentThread();

            onClose = () ->
            {
                // Restore the outer transaction
                statementContext.bindTransactionToCurrentThread( outerTx );
            };
        }
        else
        {
            onClose = () -> {};
        }

        Transaction transaction = getSystemDb().beginTx();

        return new Transaction()
        {
            @Override
            public void terminate()
            {
                transaction.terminate();
            }

            @Override
            public void failure()
            {
                transaction.failure();
            }

            @Override
            public void success()
            {
                transaction.success();
            }

            @Override
            public void close()
            {
                try
                {
                    transaction.close();
                }
                finally
                {
                    statementContext.unbindTransactionFromCurrentThread();
                    onClose.run();
                }
            }

            @Override
            public Lock acquireWriteLock( PropertyContainer entity )
            {
                return transaction.acquireWriteLock( entity );
            }

            @Override
            public Lock acquireReadLock( PropertyContainer entity )
            {
                return transaction.acquireReadLock( entity );
            }
        };
    }

    protected ThreadToStatementContextBridge getThreadToStatementContextBridge()
    {
        // Resolve statementContext of the active database on the first call
        if ( threadToStatementContextBridge == null )
        {
            GraphDatabaseFacade activeDb = getDb( activeDbName );
            threadToStatementContextBridge = activeDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        }
        return threadToStatementContextBridge;
    }

    private GraphDatabaseFacade getSystemDb()
    {
        // Resolve systemDb on the first call
        if ( systemDb == null )
        {
            systemDb = getDb( SYSTEM_DATABASE_NAME );
        }
        return systemDb;
    }

    private GraphDatabaseFacade getDb( String dbName )
    {
        return databaseManager.getDatabaseFacade( dbName )
                .orElseThrow( () -> new AuthProviderFailedException( "No database called `" + dbName + "` was found." ) );
    }
}
