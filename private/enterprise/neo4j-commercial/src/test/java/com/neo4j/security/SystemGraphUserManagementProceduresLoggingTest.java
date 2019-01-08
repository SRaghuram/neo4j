/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.UserManagementProceduresLoggingTest;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.server.security.enterprise.systemgraph.QueryExecutor;
import org.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import org.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import org.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import org.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import org.neo4j.test.rule.TestDirectory;

public class SystemGraphUserManagementProceduresLoggingTest extends UserManagementProceduresLoggingTest
{
    private GraphDatabaseService database;
    private DatabaseManager databaseManager;
    private String activeDbName;

    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Before
    @Override
    public void setUp() throws Throwable
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        File storeDir = testDirectory.databaseDir();
        final GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        database = builder.newGraphDatabase();
        activeDbName = ((GraphDatabaseFacade) database).databaseLayout().getDatabaseName();
        databaseManager = getDatabaseManager();
        super.setUp();
    }

    @After
    public void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
        }
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    @Override
    protected EnterpriseUserManager getUserManager() throws Throwable
    {
        SystemGraphImportOptions importOptions =
                new SystemGraphImportOptions( false, false, false, false, () -> new InMemoryUserRepository(), () -> new InMemoryRoleRepository(),
                        () -> new InMemoryUserRepository(), () -> new InMemoryRoleRepository(), () -> new InMemoryUserRepository(),
                        () -> new InMemoryUserRepository() );

        QueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, activeDbName );
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( queryExecutor, secureHasher );

        SystemGraphRealm realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( queryExecutor, systemGraphOperations, importOptions, secureHasher, authProcedures.securityLog ),
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                Mockito.mock( AuthenticationStrategy.class ),
                true,
                true
        );
        realm.initialize();
        realm.start(); // creates default user and roles
        return realm;
    }
}
