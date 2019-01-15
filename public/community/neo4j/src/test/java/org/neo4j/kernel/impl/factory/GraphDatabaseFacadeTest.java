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
package org.neo4j.kernel.impl.factory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;

class GraphDatabaseFacadeTest
{
    private final GraphDatabaseFacade.SPI spi = Mockito.mock( GraphDatabaseFacade.SPI.class, RETURNS_DEEP_STUBS );
    private final GraphDatabaseFacade graphDatabaseFacade = new GraphDatabaseFacade();
    private GraphDatabaseQueryService queryService;

    @BeforeEach
    void setUp()
    {
        queryService = mock( GraphDatabaseQueryService.class );
        DependencyResolver resolver = mock( DependencyResolver.class );
        Statement statement = mock( Statement.class, RETURNS_DEEP_STUBS );
        ThreadToStatementContextBridge contextBridge = mock( ThreadToStatementContextBridge.class );

        when( spi.queryService() ).thenReturn( queryService );
        when( spi.resolver() ).thenReturn( resolver );
        when( resolver.resolveDependency( ThreadToStatementContextBridge.class ) ).thenReturn( contextBridge );
        when( contextBridge.get() ).thenReturn( statement );
        Config config = Config.defaults();
        when( resolver.resolveDependency( Config.class ) ).thenReturn( config );

        graphDatabaseFacade.init( spi, contextBridge, config, mockedTokenHolders() );
    }

    @Test
    void beginTransactionWithCustomTimeout()
    {
        graphDatabaseFacade.beginTx( 10, TimeUnit.MILLISECONDS );

        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 10L );
    }

    @Test
    void beginTransaction()
    {
        graphDatabaseFacade.beginTx();

        long timeout = Config.defaults().get( GraphDatabaseSettings.transaction_timeout ).toMillis();
        verify( spi ).beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout );
    }

    @Test
    void executeQueryWithCustomTimeoutShouldStartTransactionWithRequestedTimeout()
    {
        graphDatabaseFacade.execute( "create (n)", 157L, TimeUnit.SECONDS );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION,
            TimeUnit.SECONDS.toMillis( 157L ) );

        graphDatabaseFacade.execute( "create (n)", new HashMap<>(), 247L, TimeUnit.MINUTES );
        verify( spi ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION,
            TimeUnit.MINUTES.toMillis( 247L ) );
    }

    @Test
    void executeQueryStartDefaultTransaction()
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        InternalTransaction transaction = new TopLevelTransaction( kernelTransaction );

        when( queryService.beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION) )
            .thenReturn( transaction );

        graphDatabaseFacade.execute( "create (n)" );
        graphDatabaseFacade.execute( "create (n)", new HashMap<>() );

        long timeout = Config.defaults().get( GraphDatabaseSettings.transaction_timeout ).toMillis();
        verify( spi, times( 2 ) ).beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, EMBEDDED_CONNECTION, timeout );
    }
}
