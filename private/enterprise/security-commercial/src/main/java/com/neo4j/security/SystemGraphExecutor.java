/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.dbms.database.MultiDatabaseManager;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.cypher.internal.javacompat.QueryResultProvider;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthProviderFailedException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SystemGraphExecutor
{
    private DatabaseManager databaseManager;
    private final String activeDbName;
    private GraphDatabaseFacade systemDb;
    private ThreadToStatementContextBridge statementContext;

    SystemGraphExecutor( DatabaseManager databaseManager, String activeDbName )
    {
        this.databaseManager = databaseManager;
        this.activeDbName = activeDbName;
    }

    long executeQueryLong( String query )
    {
        long[] count = new long[1];

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            count[0] = ((NumberValue) row.fields()[0]).longValue();
            return false;
        };

        executeQuery( query, Collections.emptyMap(), resultVisitor );
        return count[0];
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
        boolean[] paramCheck = { false };

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            paramCheck[0] = true; // If we get a result row, we know that the user and/or role specified in the params exist
            return true;
        };

        executeQuery( query, params, resultVisitor );
        return paramCheck[0];
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
        boolean[] success = { false };
        Set<String> resultSet = new TreeSet<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            success[0] = true; // If we get a row we know that the parameter existed in the system db
            Value value = (Value) row.fields()[0];
            if ( value != Values.NO_VALUE )
            {
                resultSet.add( ((TextValue) value).stringValue() );
            }
            return true;
        };

        executeQuery( query, params, resultVisitor );

        if ( !success[0] )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return resultSet;
    }

    void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor )
    {
        // resolve statementContext and systemDb on the first call
        if ( statementContext == null )
        {
            GraphDatabaseFacade activeDb = getDb( activeDbName );
            systemDb = getDb( MultiDatabaseManager.SYSTEM_DB_NAME );
            statementContext = activeDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        }

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
        try ( Transaction transaction = systemDb.beginTx() )
        {
            Result result = systemDb.execute( query, parameters );
            QueryResult queryResult = ((QueryResultProvider) result).queryResult();
            queryResult.accept( resultVisitor );
            transaction.success();
        }
    }

    private GraphDatabaseFacade getDb( String dbName )
    {
        return  databaseManager.getDatabaseFacade( dbName )
                .orElseThrow( () -> new AuthProviderFailedException( "No database called `" + dbName + "` was found." ) );
    }
}
