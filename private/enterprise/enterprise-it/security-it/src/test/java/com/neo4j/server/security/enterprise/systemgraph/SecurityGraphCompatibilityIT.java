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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
class SecurityGraphCompatibilityIT
{
    @SuppressWarnings( "unused" )
    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private GraphDatabaseService system;
    private EnterpriseSecurityGraphComponent enterpriseComponent;
    private AuthManager authManager;

    @BeforeEach
    void setup() throws Exception
    {
        FileUtils.deleteRecursively( directory.homeDir() );

        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestDBMSBuilder( directory.homeDir() ).impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, TRUE )
                        .setConfig( GraphDatabaseSettings.allow_single_automatic_upgrade, FALSE );
        dbms = builder.build();
        system = dbms.database( SYSTEM_DATABASE_NAME );
        DependencyResolver platformDependencies = ((GraphDatabaseAPI) system).getDependencyResolver();
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

    @Test
    void shouldAuthenticateOn36() throws Exception
    {
        initEnterprise( "Neo4j 3.6" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldNotAuthorizeOn36() throws Exception
    {
        initEnterprise( "Neo4j 3.6" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.SUCCESS );

        // Access to System is allowed but with no other privileges
        SecurityContext securityContextSystem = loginContext.authorize( LoginContext.IdLookup.EMPTY, SYSTEM_DATABASE_NAME );
        var systemMode = securityContextSystem.mode();
        Assertions.assertThat( systemMode.allowsReadPropertyAllLabels( -1 ) ).isFalse();
        Assertions.assertThat( systemMode.allowsTraverseAllLabels() ).isFalse();
        Assertions.assertThat( systemMode.allowsWrites() ).isFalse();

        // Access to neo4j is disallowed
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn40() throws Exception
    {
        initEnterprise( "Neo4j 4.0" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn40() throws Exception
    {
        initEnterprise( "Neo4j 4.0" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn41d01() throws Exception
    {
        initEnterprise( "Neo4j 4.1.0-Drop01" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn41d01() throws Exception
    {
        initEnterprise( "Neo4j 4.1.0-Drop01" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn41d02() throws Exception
    {
        initEnterprise( "Neo4j 4.1.0-Drop02" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        Assertions.assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn41d02() throws Exception
    {
        initEnterprise( "Neo4j 4.1.0-Drop02" );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    private void assertReadWritePrivileges( SecurityContext securityContext )
    {
        Assertions.assertThat( securityContext.mode().allowsReadPropertyAllLabels( -1 ) ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsTraverseAllLabels() ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsWrites() ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isTrue();
        Assertions.assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isTrue();
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
