/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphInitializer;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.QueryExecutor;

import static org.mockito.Mockito.mock;

class TestBasicSystemGraphRealm
{
    static BasicSystemGraphRealm testRealm( BasicImportOptionsBuilder importOptions, DatabaseManager<?> dbManager ) throws Throwable
    {
        ContextSwitchingSystemGraphQueryExecutor executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, new TestThreadToStatementContextBridge() );
        return testRealm( importOptions, newRateLimitedAuthStrategy(), executor );
    }

    private static BasicSystemGraphRealm testRealm(
            BasicImportOptionsBuilder importOptions,
            AuthenticationStrategy authStrategy,
            QueryExecutor executor ) throws Throwable
    {
        SecureHasher secureHasher = new SecureHasher();
        BasicSystemGraphOperations systemGraphOperations = new BasicSystemGraphOperations( executor, secureHasher );
        BasicSystemGraphInitializer systemGraphInitializer =
                new BasicSystemGraphInitializer(
                        executor,
                        systemGraphOperations,
                        importOptions.migrationSupplier(),
                        importOptions.initalUserSupplier(),
                        secureHasher,
                        mock(Log.class)
                );

        BasicSystemGraphRealm realm = new BasicSystemGraphRealm(
                systemGraphOperations,
                systemGraphInitializer,
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                authStrategy,
                true
        );
        realm.start();

        return realm;
    }

    static AuthenticationStrategy newRateLimitedAuthStrategy()
    {
        return new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    protected static class TestThreadToStatementContextBridge extends ThreadToStatementContextBridge
    {
        @Override
        public boolean hasTransaction()
        {
            return false;
        }

        @Override
        public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict )
        {
            return null;
        }
    }
}
