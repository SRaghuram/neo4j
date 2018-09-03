/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SystemGraphExecutor
{
    private final DatabaseManager databaseManager;
    private final String activeDbName;
    private GraphDatabaseFacade systemDb;
    private ThreadToStatementContextBridge threadToStatementContextBridge;

    SystemGraphExecutor( DatabaseManager databaseManager, String activeDbName )
    {
        this.databaseManager = databaseManager;
        this.activeDbName = activeDbName;
    }

    long executeQueryLong( String query )
    {
        MutableLong count = new MutableLong();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            count.setValue( ((NumberValue) row.fields()[0]).longValue() );
            return false;
        };

        executeQuery( query, Collections.emptyMap(), resultVisitor );
        return count.getValue();
    }

    void executeQueryWithConstraint( String query, Map<String,Object> params, String failureMessage ) throws InvalidArgumentsException
    {
        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;

        try
        {
            executeQuery( query, params, resultVisitor );
        }
        catch ( Exception e )
        {
            if ( e instanceof QueryExecutionException &&
                    ( (QueryExecutionException) e).getStatusCode().contains( "ConstraintValidationFailed" ) )
            {
                throw new InvalidArgumentsException( failureMessage );
            }
            throw e;
        }
    }

    boolean executeQueryWithParamCheck( String query, Map<String,Object> params )
    {
        MutableBoolean paramCheck = new MutableBoolean( false );

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            paramCheck.setTrue(); // If we get a result row, we know that the user and/or role specified in the params exist
            return true;
        };

        executeQuery( query, params, resultVisitor );
        return paramCheck.getValue();
    }

    boolean executeQueryWithParamCheck( String query, Map<String,Object> params, String errorMsg ) throws InvalidArgumentsException
    {
        boolean paramCheck = executeQueryWithParamCheck( query, params );

        if ( !paramCheck )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return true;
    }

    Set<String> executeQueryWithResultSet( String query )
    {
        Set<String> resultSet = new TreeSet<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            resultSet.add( ((TextValue) row.fields()[0]).stringValue() );
            return true;
        };

        executeQuery( query, Collections.emptyMap(), resultVisitor );
        return resultSet;
    }

    Set<String> executeQueryWithResultSetAndParamCheck( String query, Map<String,Object> params, String errorMsg ) throws InvalidArgumentsException
    {
        MutableBoolean success = new MutableBoolean( false );
        Set<String> resultSet = new TreeSet<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            success.setTrue(); // If we get a row we know that the parameter existed in the system db
            Value value = (Value) row.fields()[0];
            if ( value != Values.NO_VALUE )
            {
                resultSet.add( ((TextValue) value).stringValue() );
            }
            return true;
        };

        executeQuery( query, params, resultVisitor );

        if ( success.isFalse() )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return resultSet;
    }

    void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor )
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
        try ( Transaction transaction = getSystemDb().beginTx() ) // TODO: REV Is it really safe begin transaction with AUTH_DISABLED here?
        {
            systemDbExecuteWithinTransaction( query, parameters, resultVisitor );
            transaction.success();
        }
    }

    private void systemDbExecuteWithinTransaction( String query, Map<String,Object> parameters, QueryResult.QueryResultVisitor resultVisitor )
    {
        Result result = getSystemDb().execute( query, parameters );
        QueryResult queryResult = ((QueryResultProvider) result).queryResult();
        queryResult.accept( resultVisitor );
    }

    Transaction systemDbBeginTransaction()
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
                statementContext.unbindTransactionFromCurrentThread();
                statementContext.bindTransactionToCurrentThread( outerTx );
            };
        }
        else
        {
            onClose = () -> {};
        }

        Transaction transaction = systemDb.beginTx();

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
                //assert activeSystemDbTransaction == transaction;
                //activeSystemDbTransaction = null;

                try
                {
                    transaction.close();
                }
                finally
                {
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
        // resolve statementContext and systemDb on the first call
        if ( threadToStatementContextBridge == null )
        {
            GraphDatabaseFacade activeDb = getDb( activeDbName );
            systemDb = getDb( SYSTEM_DATABASE_NAME );
            threadToStatementContextBridge = activeDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        }
        return threadToStatementContextBridge;
    }

    private GraphDatabaseFacade getSystemDb()
    {
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
