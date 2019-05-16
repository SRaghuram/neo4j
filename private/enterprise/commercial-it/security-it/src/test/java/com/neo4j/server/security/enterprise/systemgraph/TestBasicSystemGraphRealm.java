/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.time.Clock;
import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.BasicSystemGraphInitializer;
import org.neo4j.server.security.systemgraph.BasicSystemGraphOperations;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.QueryExecutor;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.TestDatabaseManager;
import static org.mockito.Mockito.mock;

class TestBasicSystemGraphRealm
{
    private static GlobalTransactionEventListeners transactionEventListeners;
    private static Collection<TransactionEventListener<?>> systemListeners;
    private static final TestThreadToStatementContextBridge threadToStatementContextBridge = new TestThreadToStatementContextBridge();

    static final SecureHasher secureHasher = new SecureHasher();

    static BasicSystemGraphRealm testRealm( BasicImportOptionsBuilder importOptions, TestDatabaseManager dbManager, Config config ) throws Throwable
    {
        ContextSwitchingSystemGraphQueryExecutor executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, threadToStatementContextBridge );
        return testRealm( importOptions.migrationSupplier(), importOptions.initialUserSupplier(), newRateLimitedAuthStrategy(),
                dbManager.getManagementService(), executor, config );
    }

    static BasicSystemGraphRealm testRealm( TestDatabaseManager dbManager, TestDirectory testDirectory, Config config ) throws Throwable
    {
        config.augment(  DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ).toString()  );
        LogProvider logProvider = mock(LogProvider.class);
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();

        Supplier<UserRepository> migrationUserRepositorySupplier = () -> CommunitySecurityModule.getUserRepository( config, logProvider, fileSystem );
        Supplier<UserRepository> initialUserRepositorySupplier = () -> CommunitySecurityModule.getInitialUserRepository( config, logProvider, fileSystem );

        ContextSwitchingSystemGraphQueryExecutor executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, threadToStatementContextBridge );
        return testRealm( migrationUserRepositorySupplier, initialUserRepositorySupplier, newRateLimitedAuthStrategy(), dbManager.getManagementService(),
                executor, config );
    }

    private static BasicSystemGraphRealm testRealm(
            Supplier<UserRepository> migrationSupplier,
            Supplier<UserRepository> initialUserSupplier,
            AuthenticationStrategy authStrategy,
            DatabaseManagementService managementService,
            QueryExecutor executor,
            Config config ) throws Throwable
    {
        unregisterListeners( managementService, config );

        BasicSystemGraphOperations systemGraphOperations = new BasicSystemGraphOperations( executor, secureHasher );
        BasicSystemGraphInitializer systemGraphInitializer =
                new BasicSystemGraphInitializer(
                        executor,
                        systemGraphOperations,
                        migrationSupplier,
                        initialUserSupplier,
                        secureHasher,
                        mock(Log.class),
                        config
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

        registerListeners();

        return realm;
    }

    static void unregisterListeners( DatabaseManagementService managementService, Config config )
    {
        GraphDatabaseCypherService graph = new GraphDatabaseCypherService( managementService.database( config.get( GraphDatabaseSettings.default_database ) ) );
        transactionEventListeners = graph.getDependencyResolver().resolveDependency( GlobalTransactionEventListeners.class );
        systemListeners = transactionEventListeners.getDatabaseTransactionEventListeners( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

        for ( TransactionEventListener<?> listener : systemListeners )
        {
            transactionEventListeners.unregisterTransactionEventListener( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, listener );
        }
    }

    static void registerListeners()
    {
        for ( TransactionEventListener<?> listener : systemListeners )
        {
            transactionEventListeners.registerTransactionEventListener( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, listener );
        }
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
