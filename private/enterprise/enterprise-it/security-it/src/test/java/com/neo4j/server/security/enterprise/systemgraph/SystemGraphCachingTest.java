/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.systemgraph.ErrorPreservingQuerySubscriber;
import org.neo4j.server.security.systemgraph.SystemGraphQueryExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class SystemGraphCachingTest
{
    private GraphDatabaseService database;
    private TestQueryExecutor systemGraphExecutor;
    private SystemGraphRealm realm;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws Throwable
    {
        final DatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() );
        builder.setConfig( SecuritySettings.authentication_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) );
        builder.setConfig( SecuritySettings.authorization_providers, List.of( SecuritySettings.NATIVE_REALM_NAME ) );
        managementService = builder.build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        DatabaseManager<?> databaseManager = dependencyResolver.resolveDependency( DatabaseManager.class );
        systemGraphExecutor = new TestQueryExecutor( databaseManager );
        SecurityLog securityLog = new SecurityLog( new AssertableLogProvider().getLog( getClass() ) );

        realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder().build(),
                securityLog, databaseManager, systemGraphExecutor, Config.defaults() );
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            managementService.shutdown();
            database = null;
        }
    }

    @Test
    void shouldCachePrivilegeForRole()
    {
        // Given
        systemGraphExecutor.takeAccessFlag();
        realm.clearCacheForRoles();

        // When
        realm.getPrivilegesForRoles( Set.of( READER ) );

        // Then
        assertTrue( systemGraphExecutor.takeAccessFlag(), "Should have looked up privilege for role in system graph" );

        // When
        realm.getPrivilegesForRoles( Set.of( READER ) );

        // Then
        assertFalse( systemGraphExecutor.takeAccessFlag(), "Should have looked up privilege for role in cache" );
    }

    @Test
    void shouldUseCacheForMultipleRoles()
    {
        // Given
        realm.getPrivilegesForRoles( Set.of( READER ) );
        realm.clearCacheForRoles();
        systemGraphExecutor.takeAccessFlag();

        // When
        realm.getPrivilegesForRoles( Set.of( READER, EDITOR ) );

        // Then
        assertTrue( systemGraphExecutor.takeAccessFlag(), "Should have looked up privilege for roles in system graph" );

        // When
        realm.getPrivilegesForRoles( Set.of( READER, EDITOR ) );

        // Then
        assertFalse( systemGraphExecutor.takeAccessFlag(), "Should have looked up privilege for roles in cache" );
    }

    private static class TestQueryExecutor extends SystemGraphQueryExecutor
    {
        private boolean systemAccess;

        TestQueryExecutor( DatabaseManager<?> databaseManager )
        {
            super( databaseManager );
        }

        boolean takeAccessFlag()
        {
            boolean access = systemAccess;
            systemAccess = false;
            return access;
        }

        @Override
        public void executeQuery( String query, Map<String,Object> params, ErrorPreservingQuerySubscriber subscriber )
        {
            systemAccess = true;
            super.executeQuery( query, params, subscriber );
        }
    }
}
