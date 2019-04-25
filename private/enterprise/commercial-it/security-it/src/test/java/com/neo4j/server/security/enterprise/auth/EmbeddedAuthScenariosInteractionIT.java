/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public class EmbeddedAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<CommercialLoginContext>
{
    @Override
    protected NeoInteractionLevel<CommercialLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Test
    void shouldAllowReadsForCustomRoleWithReadPrivilege() throws Throwable
    {
        // Given
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "CustomRead", "Alice" );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        userManager.grantPrivilegeToRole( "CustomRead", dbPriv );

        // Then
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        testSuccessfulRead( subject, 3 );
        testFailWrite( subject );
    }

    @Test
    void shouldRevokePrivilegeFromRole() throws Throwable
    {
        // Given
        String roleName = "CustomRole";
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( roleName, "Alice" );

        // When
        CommercialLoginContext subject = neo.login( "Alice", "foo" );

        // Then
        testFailRead( subject, 3 );
        testFailWrite( subject );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );
        userManager.grantPrivilegeToRole( roleName, dbPriv );

        // Then
        testSuccessfulRead( subject, 3 );
        testSuccessfulWrite( subject );

        // When
        dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );
        userManager.revokePrivilegeFromRole( roleName, dbPriv );

        // Then
        testSuccessfulRead( subject, 4 );
        testFailWrite( subject );
    }

    @Test
    void shouldAllowUserManagementForCustomRoleWithAdminPrivilege() throws Throwable
    {
        // Given
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "UserManager", "Alice" );

        // When
        userManager.setAdmin( "UserManager", true );

        // Then
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertEmpty( subject, "CALL dbms.security.createUser('Bob', 'bar', false)" );
        testFailRead( subject, 3 );
    }

    @Test
    void shouldNotAllowChangingBuiltinRoles() throws InvalidArgumentsException
    {
        DatabasePrivilege privilege = new DatabasePrivilege( "*" );
        privilege.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );

        for ( String role : Arrays.asList( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER, PredefinedRoles.EDITOR,
                PredefinedRoles.READER ) )
        {
            assertThrows( InvalidArgumentsException.class, () -> userManager.setAdmin( role, true ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.setAdmin( role, false ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.revokePrivilegeFromRole( role, privilege ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.grantPrivilegeToRole( role, privilege ) );
        }
    }
}
