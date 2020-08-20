/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.server.security.enterprise.systemgraph.versions.PrivilegeBuilder;
import com.neo4j.server.security.enterprise.systemgraph.versions.SupportedEnterpriseVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.VERSION_36;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.VERSION_40;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.VERSION_41;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.VERSION_41D1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNSUPPORTED_FUTURE;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@TestDirectoryExtension
@TestInstance( PER_CLASS )
class SystemGraphComponentsTest
{
    @Inject
    @SuppressWarnings( "unused" )
    private static TestDirectory directory;

    private static DatabaseManagementService dbms;
    private static GraphDatabaseFacade system;
    private static SystemGraphComponents systemGraphComponents;
    private static EnterpriseSecurityGraphComponent enterpriseComponent;

    @BeforeAll
    static void setup()
    {
        dbms = new TestDatabaseManagementServiceBuilder( directory.homePath() ).impermanent().noOpSystemGraphInitializer().build();
        system = (GraphDatabaseFacade) dbms.database( SYSTEM_DATABASE_NAME );
        systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );

        // This dbms builder is a community version, so we have to manually add the enterprise security component
        RoleRepository oldRoleRepo = new InMemoryRoleRepository();
        UserRepository adminRepo = new InMemoryUserRepository();
        enterpriseComponent = new EnterpriseSecurityGraphComponent( NullLog.getInstance(), oldRoleRepo, adminRepo, Config.defaults() );
        systemGraphComponents.register( enterpriseComponent );
    }

    @BeforeEach
    void clear() throws Exception
    {
        inTx( tx -> tx.getAllNodes().stream().forEach( n ->
        {
            n.getRelationships().forEach( Relationship::delete );
            n.delete();
        } ) );
    }

    @AfterAll
    static void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldInitializeDefaultVersion() throws Exception
    {
        systemGraphComponents.initializeSystemGraph( system );

        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        assertThat( "Expecting four components", statuses.size(), is( 4 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( CURRENT ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( CURRENT ) );
    }

    @ParameterizedTest
    @MethodSource( "versionAndStatusProvider" )
    void shouldInitializeAndUpgradeSystemGraph( String version, SystemGraphComponent.Status initialStatus ) throws Exception
    {
        initializeLatestSystemAndUsers();
        initEnterprise( version );
        assertCanUpgradeThisVersionAndThenUpgradeIt( initialStatus );
    }

    @ParameterizedTest
    @ValueSource( strings = {VERSION_36, VERSION_40} )
    void shouldFailUpgradeWhen_PUBLIC_RoleExists( String version ) throws Exception
    {
        SystemGraphComponent.Status initialStatus = version.equals( VERSION_36 ) ? UNSUPPORTED_BUT_CAN_UPGRADE : REQUIRES_UPGRADE;
        initializeLatestSystemAndUsers();
        initEnterprise( version, List.of( ADMIN, PUBLIC ) );

        Optional<Exception> exception = systemGraphComponents.upgradeToCurrent( system );
        exception.ifPresentOrElse(
                e -> assertThat( "Should not be able to upgrade with PUBLIC role", e.getMessage(),
                        containsString( "'PUBLIC' is a reserved role and must be dropped before upgrade can proceed" ) ),
                () -> fail( "Should have gotten an exception when upgrading" )
        );

        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", CURRENT,
                "security-privileges", initialStatus,
                "dbms-status", initialStatus
        ) );
    }

    @Test
    void shouldNotSupportFutureVersions() throws Exception
    {
        initializeLatestSystemAndUsers();
        initEnterpriseFutureUnknown();

        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", CURRENT,
                "security-privileges", UNSUPPORTED_FUTURE,
                "dbms-status", UNSUPPORTED_FUTURE
        ) );
    }

    static Stream<Arguments> versionAndStatusProvider()
    {
        return Stream.of(
                Arguments.arguments( VERSION_36, UNSUPPORTED_BUT_CAN_UPGRADE ),
                Arguments.arguments( VERSION_40, REQUIRES_UPGRADE ),
                Arguments.arguments( VERSION_41D1, REQUIRES_UPGRADE ),
                Arguments.arguments( VERSION_41, CURRENT )
        );
    }

    private void assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status initialState ) throws Exception
    {
        var systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );
        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", CURRENT,
                "security-privileges", initialState,
                "dbms-status", initialState
        ) );

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent( system );

        // Then when looking at component statuses
        assertStatus( Map.of(
                "multi-database", CURRENT,
                "security-users", CURRENT,
                "security-privileges", CURRENT,
                "dbms-status", CURRENT
        ) );

        inTx( tx ->
        {
            Node schemaNode = tx.findNode( Label.label( "Privilege" ), "action", "schema" );
            assertNull( schemaNode );
        } );
    }

    private void assertStatus( Map<String,SystemGraphComponent.Status> expected ) throws Exception
    {
        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        for ( var entry : expected.entrySet() )
        {
            assertThat( entry.getKey(), statuses.get( entry.getKey() ), is( entry.getValue() ) );
        }
    }

    private void initEnterprise( String version ) throws Exception
    {
        List<String> roles;
        switch ( version )
        {
        case VERSION_36:
        case VERSION_40:
            // Versions older than 41 drop 01 should not have PUBLIC role
            roles = List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER );
            break;
        default:
            roles = PredefinedRoles.roles;
        }
        initEnterprise( version, roles );
    }

    private void initEnterprise( String version, List<String> roles ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );
        inTx( tx -> enterpriseComponent.initializeSystemGraphConstraints( tx ) );
        inTx( tx -> builder.initializePrivileges( tx, roles, Map.of( ADMIN, Set.of( INITIAL_USER_NAME ) ) ) );
    }

    private void initEnterpriseFutureUnknown() throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = new EnterpriseVersionFake( mock( Log.class ) );
        inTx( tx -> builder.initializePrivileges( tx, PredefinedRoles.roles, Map.of( ADMIN, Set.of( INITIAL_USER_NAME ) ) ) );
    }

    private void initializeLatestSystemAndUsers()
    {
        var systemGraphComponent = new DefaultSystemGraphComponent( Config.defaults() );
        UserRepository oldUsers = new InMemoryUserRepository();
        UserRepository initialPassword = new InMemoryUserRepository();
        var userSecurityGraphComponent = new UserSecurityGraphComponent( NullLog.getInstance(), oldUsers, initialPassword, Config.defaults() );

        systemGraphComponent.initializeSystemGraph( system );
        userSecurityGraphComponent.initializeSystemGraph( system );
    }

    private void inTx( ThrowingConsumer<Transaction,Exception> consumer ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            consumer.accept( tx );
            tx.commit();
        }
    }

    private static class EnterpriseVersionFake extends SupportedEnterpriseVersion
    {
        EnterpriseVersionFake( Log log )
        {
            super( Integer.MAX_VALUE, "Neo4j 8.8.88", log, true );
        }

        @Override
        public boolean migrationSupported()
        {
            return true;
        }

        @Override
        public boolean runtimeSupported()
        {
            return true;
        }

        @Override
        public void setUpDefaultPrivileges( Transaction tx )
        {
            super.setUpDefaultPrivileges( tx );
            this.setVersionProperty( tx, version );
        }

        @Override
        public void assertUpdateWithAction( PrivilegeAction action, ResourcePrivilege.SpecialDatabase specialDatabase ) throws UnsupportedOperationException
        {
            // Current version supports all current commands
        }

        @Override
        public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
        {
            return super.currentGetPrivilegeForRoles( tx, roleNames, privilegeCache );
        }

        @Override
        public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
        {
            return new PrivilegeBuilder( privilegeType, action );
        }
    }
}
