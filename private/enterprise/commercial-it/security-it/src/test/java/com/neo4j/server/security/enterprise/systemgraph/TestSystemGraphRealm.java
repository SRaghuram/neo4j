/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.neo4j.server.security.auth.SecureHasher;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.QueryExecutor;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestSystemGraphRealm
{
    public static SystemGraphRealm testRealm( SecurityLog securityLog, QueryExecutor queryExecutor ) throws Throwable
    {
        return testRealm( new ImportOptionsBuilder().build(), securityLog, newRateLimitedAuthStrategy(), queryExecutor );
    }

    static SystemGraphRealm testRealm( SystemGraphImportOptions importOptions, SecurityLog securityLog, DatabaseManager<?> dbManager )
            throws Throwable
    {
        ContextSwitchingSystemGraphQueryExecutor executor = new TestContextSwitchingSystemGraphQueryExecutor( dbManager );
        return testRealm( importOptions, securityLog, newRateLimitedAuthStrategy(), executor );
    }

    private static SystemGraphRealm testRealm(
            SystemGraphImportOptions importOptions,
            SecurityLog securityLog,
            AuthenticationStrategy authStrategy,
            QueryExecutor executor ) throws Throwable
    {
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( executor, secureHasher );
        SystemGraphRealm realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( executor, systemGraphOperations, importOptions, secureHasher, securityLog ),
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                authStrategy,
                true,
                true
        );

        realm.initialize();
        realm.start();

        return realm;
    }

    private static AuthenticationStrategy newRateLimitedAuthStrategy()
    {
        return new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    private static class TestContextSwitchingSystemGraphQueryExecutor extends ContextSwitchingSystemGraphQueryExecutor
    {
        private TestThreadToStatementContextBridge bridge;

        TestContextSwitchingSystemGraphQueryExecutor( DatabaseManager<?> databaseManager )
        {
            super( databaseManager, DEFAULT_DATABASE_NAME );
            bridge = new TestThreadToStatementContextBridge();
        }

        @Override
        protected ThreadToStatementContextBridge getThreadToStatementContextBridge()
        {
            return bridge;
        }
    }

    private static class TestThreadToStatementContextBridge extends ThreadToStatementContextBridge
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
