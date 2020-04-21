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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
class EnterpriseSecurityGraphInitializerTest
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private GraphDatabaseService system;
    private SystemGraphComponents systemGraphComponents;
    private EnterpriseSecurityGraphComponent enterpriseComponent;

    @BeforeEach
    void setup() throws Exception
    {
        UserRepository userRepository = new InMemoryUserRepository();
        RoleRepository roleRepository = new InMemoryRoleRepository();

        FileUtils.deleteRecursively( directory.homeDir() );
        Log securityLog = mock( Log.class );
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestDBMSBuilder( directory.homeDir() ).impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, TRUE )
                        .setConfig( GraphDatabaseSettings.allow_single_automatic_upgrade, FALSE );
        Config config = ((TestDBMSBuilder) builder).getConfig();
        dbms = builder.build();
        system = dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver platformDependencies = ((GraphDatabaseAPI) system).getDependencyResolver();
        systemGraphComponents = platformDependencies.resolveDependency( SystemGraphComponents.class );
        var communityComponent = new UserSecurityGraphComponent( securityLog, userRepository, userRepository, config );
        enterpriseComponent = new EnterpriseSecurityGraphComponent( securityLog, roleRepository, userRepository, config );
        initializeDatabasesAndUsersOnly( communityComponent, config );
    }

    private void initializeDatabasesAndUsersOnly( UserSecurityGraphComponent communityComponent, Config config ) throws Exception
    {
        var componentsToInitialize = new SystemGraphComponents();
        componentsToInitialize.register( new DefaultSystemGraphComponent( config ) );
        componentsToInitialize.register( communityComponent );
        new DefaultSystemGraphInitializer( () -> system, componentsToInitialize ).start();
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

    @Test
    void shouldInitializeDefaultVersion()
    {
        enterpriseComponent.initializeSystemGraph( system );

        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        SystemGraphComponent.Status dbmsStatus;
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            dbmsStatus = systemGraphComponents.detect( tx );
        }
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Overall status", dbmsStatus, is( SystemGraphComponent.Status.CURRENT ) );
    }

    @Test
    void shouldInitializeWith40_systemGraph() throws Exception
    {
        initEnterprise( "Neo4j 4.0" );

        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        SystemGraphComponent.Status dbmsStatus;
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            dbmsStatus = systemGraphComponents.detect( tx );
        }
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.REQUIRES_UPGRADE ) );
        assertThat( "Overall status", dbmsStatus, is( SystemGraphComponent.Status.REQUIRES_UPGRADE ) );

        // When running dbms.upgrade
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.upgradeToCurrent( tx );
            tx.commit();
        }

        // Then when looking at component statuses
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            dbmsStatus = systemGraphComponents.detect( tx );
        }
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Overall status", dbmsStatus, is( SystemGraphComponent.Status.CURRENT ) );
    }

    @Test
    void shouldInitializeWith36_systemGraph() throws Exception
    {
        initEnterprise( "Neo4j 3.6" );

        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        SystemGraphComponent.Status dbmsStatus;
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            dbmsStatus = systemGraphComponents.detect( tx );
        }
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE ) );
        assertThat( "Overall status", dbmsStatus, is( SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE ) );

        // When running dbms.upgrade
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.upgradeToCurrent( tx );
            tx.commit();
        }

        // Then when looking at component statuses
        try ( Transaction tx = system.beginTx() )
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            dbmsStatus = systemGraphComponents.detect( tx );
        }
        assertThat( "Expecting three components", statuses.size(), is( 3 ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Overall status", dbmsStatus, is( SystemGraphComponent.Status.CURRENT ) );
    }

    private void initEnterprise( String version ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );
        try ( Transaction tx = system.beginTx() )
        {
            builder.initializePrivileges( tx, PredefinedRoles.roles, Map.of( PredefinedRoles.ADMIN, Set.of( "neo4j" ) ) );
            tx.commit();
        }
    }

    private static class TestDBMSBuilder extends TestEnterpriseDatabaseManagementServiceBuilder
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

            Dependencies deps = new Dependencies( dependencies );
            deps.satisfyDependencies( SystemGraphInitializer.NO_OP, new TestDatabaseIdRepository( cfg ) );
            dependencies = deps;

            return newDatabaseManagementService( cfg, databaseDependencies() );
        }
    }
}
