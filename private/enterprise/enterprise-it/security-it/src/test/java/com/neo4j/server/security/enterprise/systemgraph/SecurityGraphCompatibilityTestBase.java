/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
abstract class SecurityGraphCompatibilityTestBase
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory directory;

    static final String VERSION_36 = "Neo4j 3.6";
    static final String VERSION_40 = "Neo4j 4.0";
    static final String VERSION_41D1 = "Neo4j 4.1.0-Drop01";
    static final String VERSION_41 = "Neo4j 4.1";

    private DatabaseManagementService dbms;
    private EnterpriseSecurityGraphComponent enterpriseComponent;

    GraphDatabaseAPI system;
    AuthManager authManager;

    @BeforeEach
    void setup() throws Exception
    {
        FileUtils.deleteRecursively( directory.homeDir() );

        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestDBMSBuilder( directory.homeDir() ).impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, TRUE )
                        .setConfig( GraphDatabaseSettings.allow_single_automatic_upgrade, FALSE );
        dbms = builder.build();
        system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver platformDependencies = system.getDependencyResolver();
        authManager = platformDependencies.resolveDependency( AuthManager.class );
        initEnterprise();
    }

    void initEnterprise() throws Exception
    {
    }

    @AfterEach
    void teardown()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
            dbms = null;
            system = null;
        }
    }

    void initEnterprise( String version ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );
        try ( Transaction tx = system.beginTx() )
        {
            builder.initializePrivileges( tx, PredefinedRoles.roles, Map.of( PredefinedRoles.ADMIN, Set.of( "neo4j" ) ) );
            tx.commit();
        }
    }

    private class TestDBMSBuilder extends TestEnterpriseDatabaseManagementServiceBuilder
    {
        TestDBMSBuilder( File homeDirectory )
        {
            super( homeDirectory );
        }

        public Config getConfig()
        {
            return this.config.build();
        }

        @Override
        public DatabaseManagementService build()
        {
            Config cfg = config.set( GraphDatabaseSettings.neo4j_home, homeDirectory.toPath().toAbsolutePath() ).build();

            UserRepository userRepository = new InMemoryUserRepository();
            RoleRepository roleRepository = new InMemoryRoleRepository();
            Log securityLog = mock( Log.class );
            var communityComponent = new UserSecurityGraphComponent( securityLog, userRepository, userRepository, cfg );
            enterpriseComponent = new EnterpriseSecurityGraphComponent( securityLog, roleRepository, userRepository, cfg );
            var testSystemGraphComponents = new TestSystemGraphComponents( new DefaultSystemGraphComponent( cfg ), communityComponent );
            //testSystemGraphComponents.registerTest( enterpriseComponent );

            Dependencies deps = new Dependencies( dependencies );
            deps.satisfyDependencies( testSystemGraphComponents );
            dependencies = deps;

            return newDatabaseManagementService( cfg, databaseDependencies() );
        }
    }

    public static class TestSystemGraphComponents extends SystemGraphComponents
    {
        TestSystemGraphComponents( SystemGraphComponent... components )
        {
            for ( SystemGraphComponent component : components )
            {
                super.register( component );
            }
        }

        @Override
        public void register( SystemGraphComponent component )
        {
            // Do nothing in tests
        }
    }
}
