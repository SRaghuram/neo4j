/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import com.neo4j.server.security.enterprise.auth.UserManagementProceduresLoggingTest;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.QueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.TestSystemGraphRealm;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@ExtendWith( TestDirectoryExtension.class )
public class SystemGraphUserManagementProceduresLoggingTest extends UserManagementProceduresLoggingTest
{
    private GraphDatabaseService database;
    private DatabaseManager databaseManager;
    private String activeDbName;
    @Inject
    private TestDirectory testDirectory;

    @Override
    protected void init() throws Throwable
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        File storeDir = testDirectory.databaseDir();
        final GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        database = builder.newGraphDatabase();
        activeDbName = ((GraphDatabaseFacade) database).databaseLayout().getDatabaseName();
        databaseManager = getDatabaseManager();
        super.init();
    }

    @AfterEach
    void tearDown()
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
        QueryExecutor queryExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, activeDbName );
        return TestSystemGraphRealm.testRealm( authProcedures.securityLog, queryExecutor );
    }
}
