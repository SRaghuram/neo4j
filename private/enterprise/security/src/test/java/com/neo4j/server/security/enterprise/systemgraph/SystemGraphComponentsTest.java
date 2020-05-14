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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@TestDirectoryExtension
@TestInstance( PER_CLASS )
class SystemGraphComponentsTest
{
    @Inject
    @SuppressWarnings( "unused" )
    private static TestDirectory directory;

    private static final String VERSION_36 = "Neo4j 3.6";
    private static final String VERSION_40 = "Neo4j 4.0";
    private static final String VERSION_41D1 = "Neo4j 4.1.0-Drop01";

    private static DatabaseManagementService dbms;
    private static GraphDatabaseFacade system;
    private static SystemGraphComponents systemGraphComponents;
    private static EnterpriseSecurityGraphComponent enterpriseComponent;

    @BeforeAll
    static void setup()
    {
        dbms = new TestDatabaseManagementServiceBuilder( directory.homeDir() ).impermanent().noOpSystemGraphInitializer().build();
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
        assertThat( "System graph status", statuses.get( "multi-database" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( SystemGraphComponent.Status.CURRENT ) );
    }

    @Test
    void shouldInitializeAndUpgradeWith41_Drop01_systemGraph() throws Exception
    {
        initializeLatestSystemAndUsers();
        initEnterprise( VERSION_41D1 );
        assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status.REQUIRES_UPGRADE );
    }

    @Test
    void shouldInitializeAndUpgradeWith40_systemGraph() throws Exception
    {
        initializeLatestSystemAndUsers();
        initEnterprise( VERSION_40 );
        assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status.REQUIRES_UPGRADE );
    }

    @Test
    void shouldInitializeAndUpgradeWith36_systemGraph() throws Exception
    {
        initializeLatestSystemAndUsers();
        initEnterprise( VERSION_36 );
        assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE );
    }

    private void assertCanUpgradeThisVersionAndThenUpgradeIt( SystemGraphComponent.Status requiresUpgrade ) throws Exception
    {
        var systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );
        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        assertThat( "Expecting four components", statuses.size(), is( 4 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( requiresUpgrade ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( requiresUpgrade ) );

        // When running dbms.upgrade
        inTx( systemGraphComponents::upgradeToCurrent );

        // Then when looking at component statuses
        statuses.clear();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        assertThat( "Expecting four components", statuses.size(), is( 4 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( SystemGraphComponent.Status.CURRENT ) );

        inTx( tx -> {
            Node schemaNode = tx.findNode( Label.label( "Privilege" ), "action", "schema" );
            assertNull( schemaNode );
        } );
    }

    @Test
    void shouldNotSupportFutureVersions() throws Exception
    {
        var systemGraphComponents = system.getDependencyResolver().resolveDependency( SystemGraphComponents.class );
        initializeLatestSystemAndUsers();
        initEnterpriseFutureUnknown();
        HashMap<String,SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx( tx ->
        {
            systemGraphComponents.forEach( component -> statuses.put( component.component(), component.detect( tx ) ) );
            statuses.put( "dbms-status", systemGraphComponents.detect( tx ) );
        } );
        assertThat( "Expecting four components", statuses.size(), is( 4 ) );
        assertThat( "System graph status", statuses.get( "multi-database" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Users status", statuses.get( "security-users" ), is( SystemGraphComponent.Status.CURRENT ) );
        assertThat( "Privileges status", statuses.get( "security-privileges" ), is( SystemGraphComponent.Status.UNSUPPORTED_FUTURE ) );
        assertThat( "Overall status", statuses.get( "dbms-status" ), is( SystemGraphComponent.Status.UNSUPPORTED_FUTURE ) );
    }

    private void initEnterprise( String version ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = enterpriseComponent.findSecurityGraphComponentVersion( version );

        // Versions older than 41 drop 01 should not have PUBLIC role
//        List<String> roles = version.equals( VERSION_41D1 ) ? PredefinedRoles.roles : List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER );
        List<String> roles = PredefinedRoles.roles;
        inTx( tx -> enterpriseComponent.initializeSystemGraphConstraints( tx ) );
        inTx( tx -> builder.initializePrivileges( tx, roles, Map.of( PredefinedRoles.ADMIN, Set.of( INITIAL_USER_NAME ) ) ) );
    }

    private void initEnterpriseFutureUnknown() throws Exception
    {
        KnownEnterpriseSecurityComponentVersion builder = new EnterpriseVersionFake( mock( Log.class ) );
        inTx( tx -> builder.initializePrivileges( tx, PredefinedRoles.roles, Map.of( PredefinedRoles.ADMIN, Set.of( INITIAL_USER_NAME ) ) ) );
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
