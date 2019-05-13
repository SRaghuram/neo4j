/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.TestDatabaseManager;

import java.util.Collection;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.impl.transaction.events.GlobalTransactionEventListeners;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.SecureHasher;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.systemgraph.QueryExecutor;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestSystemGraphRealm extends TestBasicSystemGraphRealm
{

    static SystemGraphRealm testRealm( SystemGraphImportOptions importOptions, SecurityLog securityLog, TestDatabaseManager dbManager )
            throws Throwable
    {
        ContextSwitchingSystemGraphQueryExecutor executor = new ContextSwitchingSystemGraphQueryExecutor( dbManager, new TestThreadToStatementContextBridge() );
        return testRealm( importOptions, securityLog, dbManager.getManagementService(), executor );
    }

    public static SystemGraphRealm testRealm(
            SystemGraphImportOptions importOptions,
            SecurityLog securityLog,
            DatabaseManagementService managementService,
            QueryExecutor executor ) throws Throwable
    {
        GraphDatabaseCypherService graph = new GraphDatabaseCypherService( managementService.database(DEFAULT_DATABASE_NAME) );
        GlobalTransactionEventListeners transactionEventListeners = graph.getDependencyResolver().resolveDependency( GlobalTransactionEventListeners.class);
        Collection<TransactionEventListener<?>> systemListeners =
                transactionEventListeners.getDatabaseTransactionEventListeners( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );

        for ( TransactionEventListener<?> listener : systemListeners )
        {
            transactionEventListeners.unregisterTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, listener);
        }

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( executor, secureHasher );
        SystemGraphRealm realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( executor, systemGraphOperations, importOptions, secureHasher, securityLog ),
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(),
                true,
                true
        );

        realm.initialize();
        realm.start();

        for ( TransactionEventListener<?> listener : systemListeners )
        {
            transactionEventListeners.registerTransactionEventListener(GraphDatabaseSettings.SYSTEM_DATABASE_NAME, listener);
        }

        return realm;
    }
}
