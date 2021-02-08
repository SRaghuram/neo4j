/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

@TestDirectoryExtension
@TestInstance( PER_CLASS )
class EnterpriseSecurityGraphComponentTest
{
    @Inject
    private static TestDirectory directory;

    private static DatabaseManagementService dbms;
    private static GraphDatabaseService system;

    @BeforeAll
    static void setup()
    {
        dbms = new TestDatabaseManagementServiceBuilder( directory.homePath() ).impermanent().noOpSystemGraphInitializer().build();
        system = dbms.database( SYSTEM_DATABASE_NAME );
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
    void shouldDetectUninitialized() throws Exception
    {
        initializeSystemAndUsers();
        EnterpriseSecurityGraphComponent component = getComponent();
        inTx( tx ->
        {
            SystemGraphComponent.Status status = component.detect( tx );
            assertThat( status ).isEqualTo( SystemGraphComponent.Status.UNINITIALIZED );
        } );
    }

    @Test
    void shouldDetectStatusForLatest() throws Exception
    {
        // GIVEN
        initializeSystemAndUsers();
        EnterpriseSecurityGraphComponent component = getComponent();
        component.initializeSystemGraph( system, true );

        // WHEN .. THEN
        inTx( tx ->
        {
            SystemGraphComponent.Status status = component.detect( tx );
            assertThat( status ).isEqualTo( SystemGraphComponent.Status.CURRENT );
        } );
    }

    @ParameterizedTest
    @MethodSource( "versionRolesAndStatus" )
    void shouldDetectStatus( EnterpriseSecurityGraphComponentVersion version, List<String> roles, SystemGraphComponent.Status expectedStatus ) throws Exception
    {
        // GIVEN
        initializeSystemAndUsers();
        EnterpriseSecurityGraphComponent component = getComponent();
        var securityComponent = component.findSecurityGraphComponentVersion( version );
        inTx( component::initializeSystemGraphConstraints );
        Map<String,Set<String>> roleUsers = Map.of( ADMIN, Set.of( INITIAL_USER_NAME ) );
        inTx( tx -> securityComponent.initializePrivileges( tx, roles, roleUsers ) );

        // WHEN .. THEN
        inTx( tx ->
        {
            SystemGraphComponent.Status status = component.detect( tx );
            assertThat( status ).isEqualTo( expectedStatus );
        } );
    }

    private static Stream<Arguments> versionRolesAndStatus()
    {
        ArrayList<Arguments> arguments = new ArrayList<>();
        for ( var version : EnterpriseSecurityGraphComponentVersion.values() )
        {
            switch ( version )
            {
            case ENTERPRISE_SECURITY_35:
                break;
            case ENTERPRISE_SECURITY_36:
                arguments.add( Arguments.of( version, List.of( ADMIN ), UNSUPPORTED_BUT_CAN_UPGRADE ) );
                break;
            case ENTERPRISE_SECURITY_40:
                arguments.add( Arguments.of( version, List.of( ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER ), REQUIRES_UPGRADE ) );
                break;
            default:
                if ( version.runtimeSupported() )
                {
                    arguments.add( Arguments.of( version, PredefinedRoles.roles, version.isCurrent() ? CURRENT : REQUIRES_UPGRADE ) );
                }
            }
        }
        return arguments.stream();
    }

    private EnterpriseSecurityGraphComponent getComponent()
    {
        RoleRepository oldRoleRepo = new InMemoryRoleRepository();
        UserRepository adminRepo = new InMemoryUserRepository();
        return new EnterpriseSecurityGraphComponent( NullLog.getInstance(), oldRoleRepo, adminRepo, Config.defaults() );
    }

    private void initializeSystemAndUsers() throws Exception
    {
        var systemGraphComponent = new DefaultSystemGraphComponent( Config.defaults() );
        UserRepository oldUsers = new InMemoryUserRepository();
        UserRepository initialPassword = new InMemoryUserRepository();
        var userSecurityGraphComponent = new UserSecurityGraphComponent( NullLog.getInstance(), oldUsers, initialPassword, Config.defaults() );

        systemGraphComponent.initializeSystemGraph( system, true );
        userSecurityGraphComponent.initializeSystemGraph( system, true );
    }

    private void inTx( ThrowingConsumer<Transaction,Exception> consumer ) throws Exception
    {
        try ( Transaction tx = system.beginTx() )
        {
            consumer.accept( tx );
            tx.commit();
        }
    }
}
