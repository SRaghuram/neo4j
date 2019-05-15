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
package org.neo4j.server.database;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.web.HttpHeaderUtils;
import org.neo4j.values.virtual.VirtualValues;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class CypherExecutorTest
{
    private static final long CUSTOM_TRANSACTION_TIMEOUT = 1000L;
    private static final String QUERY = "create (n)";

    private Database database;
    private GraphDatabaseQueryService databaseQueryService;
    private HttpServletRequest request;
    private AssertableLogProvider logProvider;

    @Before
    public void setUp() throws KernelException
    {
        setUpMocks();
        initLogProvider();
    }

    @Test
    public void startDefaultTransaction()
    {
        CypherExecutor cypherExecutor = new CypherExecutor( database, logProvider );
        cypherExecutor.start();

        cypherExecutor.createTransactionContext( QUERY, VirtualValues.EMPTY_MAP, request );

        verify( databaseQueryService ).beginTransaction( eq( KernelTransaction.Type.implicit ), eq( AUTH_DISABLED ), any( HttpConnectionInfo.class ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void startTransactionWithCustomTimeout()
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( String.valueOf( CUSTOM_TRANSACTION_TIMEOUT ) );

        CypherExecutor cypherExecutor = new CypherExecutor( database, logProvider );
        cypherExecutor.start();

        cypherExecutor.createTransactionContext( QUERY, VirtualValues.EMPTY_MAP, request );

        verify( databaseQueryService ).beginTransaction( eq( KernelTransaction.Type.implicit ), eq( AUTH_DISABLED ), any( HttpConnectionInfo.class ),
                eq( CUSTOM_TRANSACTION_TIMEOUT ), eq( TimeUnit.MILLISECONDS ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void startDefaultTransactionWhenHeaderHasIncorrectValue()
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( "not a number" );

        CypherExecutor cypherExecutor = new CypherExecutor( database, logProvider );
        cypherExecutor.start();

        cypherExecutor.createTransactionContext( QUERY, VirtualValues.EMPTY_MAP, request );

        verify( databaseQueryService ).beginTransaction( eq( KernelTransaction.Type.implicit ), eq( AUTH_DISABLED ), any( HttpConnectionInfo.class ) );
        logProvider.assertContainsMessageContaining( "Fail to parse `max-execution-time` header with value: 'not a " +
                                                     "number'. Should be a positive number." );
    }

    @Test
    public void startDefaultTransactionIfTimeoutIsNegative()
    {
        when( request.getHeader( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER ) )
                .thenReturn( "-2" );

        CypherExecutor cypherExecutor = new CypherExecutor( database, logProvider );
        cypherExecutor.start();

        cypherExecutor.createTransactionContext( QUERY, VirtualValues.EMPTY_MAP, request );

        verify( databaseQueryService ).beginTransaction( eq( KernelTransaction.Type.implicit ), eq( AUTH_DISABLED ), any( HttpConnectionInfo.class ) );
        logProvider.assertNoLoggingOccurred();
    }

    private void initLogProvider()
    {
        logProvider = new AssertableLogProvider( true );
    }

    private void setUpMocks() throws KernelException
    {
        database = mock( Database.class );
        databaseQueryService = mock( GraphDatabaseQueryService.class );
        request = mock( HttpServletRequest.class );
        GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
        DependencyResolver resolver = mock( DependencyResolver.class );
        QueryExecutionEngine executionEngine = mock( ExecutionEngine.class );
        ThreadToStatementContextBridge statementBridge = mock( ThreadToStatementContextBridge.class );
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        Statement statement = mock( Statement.class );

        InternalTransaction transaction = new TopLevelTransaction( kernelTransaction );

        LoginContext loginContext = AUTH_DISABLED;
        KernelTransaction.Type type = KernelTransaction.Type.implicit;
        QueryRegistry registryOperations = mock( QueryRegistry.class );
        when( statement.queryRegistration() ).thenReturn( registryOperations );
        when( statementBridge.get() ).thenReturn( statement );
        when( kernelTransaction.securityContext() ).thenReturn( loginContext.authorize(
                LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ) );
        when( kernelTransaction.transactionType() ).thenReturn( type  );
        when( database.getGraph() ).thenReturn( databaseFacade );
        when( databaseFacade.getDependencyResolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( QueryExecutionEngine.class ) ).thenReturn( executionEngine );
        when( resolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( statementBridge );
        when( resolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( databaseQueryService );
        when( databaseQueryService.beginTransaction( eq( type ), eq( loginContext ), any( HttpConnectionInfo.class ) ) ).thenReturn( transaction );
        when( databaseQueryService.beginTransaction( eq( type ), eq( loginContext ), any( HttpConnectionInfo.class ), eq( CUSTOM_TRANSACTION_TIMEOUT ),
                eq( TimeUnit.MILLISECONDS ) ) ).thenReturn( transaction );
        when( databaseQueryService.getDependencyResolver() ).thenReturn( resolver );
        when( request.getScheme() ).thenReturn( "http" );
        when( request.getRemoteAddr() ).thenReturn( "127.0.0.1" );
        when( request.getRemotePort() ).thenReturn( 5678 );
        when( request.getServerName() ).thenReturn( "127.0.0.1" );
        when( request.getServerPort() ).thenReturn( 7474 );
        when( request.getRequestURI() ).thenReturn( "/" );
    }
}
