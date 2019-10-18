/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.test.rule.CommercialDatabaseRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;

public class SystemGraphRestrictedUserManagementIT
{
    private TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    private DatabaseRule dbRule =
            new CommercialDatabaseRule( testDirectory ).startLazily()
                                                       .withSetting( SecuritySettings.auth_providers, SecuritySettings.SYSTEM_GRAPH_REALM_NAME )
                                                       .withSetting( GraphDatabaseSettings.auth_enabled, "true" );

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( dbRule );

    @After
    public void tearDown()
    {
        dbRule.shutdown();
    }

    @Test
    public void shouldHaveRoleAndUserManagementProceduresByDefault()
    {
        try ( Transaction transaction = dbRule.beginTx() )
        {
            Result result = dbRule.execute( "CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'dbms.security' RETURN name" );
            ResourceIterator<String> itr = result.columnAs( "name" );
            List<String> procNames = itr.stream().collect( Collectors.toList());
            System.out.println( procNames );
            assertThat( procNames, containsInAnyOrder(
                    "dbms.security.showCurrentUser",
                    "dbms.security.createUser",
                    "dbms.security.deleteUser",
                    "dbms.security.listUsers",
                    "dbms.security.activateUser",
                    "dbms.security.suspendUser",
                    "dbms.security.changePassword",
                    "dbms.security.changeUserPassword",
                    "dbms.security.clearAuthCache",
                    // role procedures
                    "dbms.security.addRoleToUser",
                    "dbms.security.createRole",
                    "dbms.security.deleteRole",
                    "dbms.security.listRoles",
                    "dbms.security.listRolesForUser",
                    "dbms.security.listUsersForRole",
                    "dbms.security.removeRoleFromUser"
            ) );
            transaction.success();
        }
    }

    @Test
    public void shouldOnlyHaveUserManagementProceduresWithRBACDisabled()
    {
        dbRule.withSetting( SecuritySettings.restrict_rbac, "true" );
        try ( Transaction transaction = dbRule.beginTx() )
        {
            Result result = dbRule.execute( "CALL dbms.procedures() YIELD name WHERE name STARTS WITH 'dbms.security' RETURN name" );
            ResourceIterator<String> itr = result.columnAs( "name" );
            List<String> procNames = itr.stream().collect( Collectors.toList());
            assertThat( procNames, containsInAnyOrder(
                    "dbms.security.showCurrentUser",
                    "dbms.security.createUser",
                    "dbms.security.deleteUser",
                    "dbms.security.listUsers",
                    "dbms.security.activateUser",
                    "dbms.security.suspendUser",
                    "dbms.security.changePassword",
                    "dbms.security.changeUserPassword",
                    "dbms.security.clearAuthCache"
            ) );
            transaction.success();
        }
    }

    @Test
    public void shouldCreateUserWithNoRoleByDefault() throws InvalidAuthTokenException
    {
        // Have to make sure to open transaction with an EnterpriseSecurityContext, since that is needed for the procedures
        try ( Transaction transaction = dbRule.beginTransaction( explicit, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            dbRule.execute( "CALL dbms.security.createUser('foo', 'bar', false)" ).close();
            transaction.success();
        }

        EnterpriseAuthManager authManager = dbRule.resolveDependency( EnterpriseAuthManager.class );
        EnterpriseLoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "foo", "bar" ) );
        assertThat( loginContext.subject().username(), equalTo( "foo" ) );
        assertThat( loginContext.roles(), empty() );
    }

    @Test
    public void shouldCreateUserAsAdminWithRBACDisabled() throws InvalidAuthTokenException
    {
        dbRule.withSetting( SecuritySettings.restrict_rbac, "true" );
        try ( Transaction transaction = dbRule.beginTx() )
        {
            dbRule.execute( "CALL dbms.security.createUser('foo', 'bar', false)" ).close();
            transaction.success();
        }

        EnterpriseAuthManager authManager = dbRule.resolveDependency( EnterpriseAuthManager.class );
        EnterpriseLoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "foo", "bar" ) );
        assertThat( loginContext.subject().username(), equalTo( "foo" ) );
        assertThat( loginContext.roles(), equalTo( Collections.singleton( PredefinedRoles.ADMIN ) ) );
    }
}
