/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41D1;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_single_automatic_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@EphemeralTestDirectoryExtension
abstract class SecurityGraphCompatibilityTestBase
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory directory;

    protected DatabaseManagementService dbms;
    EnterpriseSecurityGraphComponent enterpriseComponent;
    private RoleRepository roleRepository;

    GraphDatabaseAPI system;
    AuthManager authManager;

    @BeforeEach
    void setup()
    {
        Config cfg = Config.newBuilder()
                .set( auth_enabled, TRUE )
                .set( allow_single_automatic_upgrade, FALSE )
                .build();

        UserRepository userRepository = new InMemoryUserRepository();
        roleRepository = new InMemoryRoleRepository();
        Log securityLog = mock( Log.class );
        var communityComponent = new UserSecurityGraphComponent( securityLog, userRepository, userRepository, cfg );
        enterpriseComponent = new EnterpriseSecurityGraphComponent( securityLog, roleRepository, userRepository, cfg );
        var testSystemGraphComponents = new TestSystemGraphComponents( new DefaultSystemGraphComponent( cfg ), communityComponent );
        // We explicitly do not add enterpriseComponents to the component initializer, so we can initialize per test class

        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( testSystemGraphComponents );

        TestEnterpriseDatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath() )
                .impermanent()
                .setConfig( cfg )
                .setExternalDependencies( deps );
        dbms = builder.build();
        system = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver platformDependencies = system.getDependencyResolver();
        authManager = platformDependencies.resolveDependency( AuthManager.class );
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

    void initEnterprise( EnterpriseSecurityGraphComponentVersion version ) throws Exception
    {
        List<String> roles;
        switch ( version )
        {
        case ENTERPRISE_SECURITY_35:
            // Version 3.5 does not have roles in the system graph, we must assign all the default roles to a user in order for them to be migrated
            roleRepository.create( new RoleRecord( ADMIN, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( ARCHITECT, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( PUBLISHER, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( EDITOR, INITIAL_USER_NAME ) );
            roleRepository.create( new RoleRecord( READER, INITIAL_USER_NAME ) );
            return;
        case ENTERPRISE_SECURITY_36:
        case ENTERPRISE_SECURITY_40:
            // Versions older than 41 drop 01 should not have PUBLIC role
            roles = List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER );
            break;
        default:
            roles = PredefinedRoles.roles;
        }
        initEnterprise( version, roles );
    }

    private void initEnterprise( EnterpriseSecurityGraphComponentVersion version, List<String> roles ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );
        try ( Transaction tx = system.beginTx() )
        {
            enterpriseComponent.initializeSystemGraphConstraints( tx );
            tx.commit();
        }
        try ( Transaction tx = system.beginTx() )
        {
            builder.initializePrivileges( tx, roles, Map.of( ADMIN, Set.of( INITIAL_USER_NAME ) ) );
            // Versions older than 41 drop 01 should not have a version set
            if ( version.compareTo( ENTERPRISE_SECURITY_41D1 ) > 0 )
            {
                builder.setVersionProperty( tx, builder.version );
            }
            tx.commit();
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

        void override( SystemGraphComponent component )
        {
            super.register( component );
        }
    }
}
