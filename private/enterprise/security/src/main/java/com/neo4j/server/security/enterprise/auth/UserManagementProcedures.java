/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.string.UTF8;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class UserManagementProcedures extends AuthProceduresBase
{

    @Admin
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        userManager.newUser( username, password != null ? UTF8.encode( password ) : null, requirePasswordChange );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = DBMS )
    public void changePassword( @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "false" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        setUserPassword( securityContext.subject().username(), password, requirePasswordChange );
    }

    @Description( "Change the given user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword( @Name( "username" ) String username, @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        securityContext.assertCredentialsNotExpired();
        setUserPassword( username, newPassword, requirePasswordChange );
    }

    @Admin
    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = DBMS )
    public void addRoleToUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws IOException, InvalidArgumentsException
    {
        userManager.addRoleToUser( roleName, username );
    }

    @Admin
    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = DBMS )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username )
            throws InvalidArgumentsException, IOException
    {
        userManager.removeRoleFromUser( roleName, username );
    }

    @Admin
    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = DBMS )
    public void deleteUser( @Name( "username" ) String username ) throws InvalidArgumentsException, IOException
    {
        if ( userManager.deleteUser( username ) )
        {
            kickoutUser( username, "deletion" );
        }
    }

    @Admin
    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = DBMS )
    public void suspendUser( @Name( "username" ) String username ) throws IOException, InvalidArgumentsException
    {
        userManager.suspendUser( username );
        kickoutUser( username, "suspension" );
    }

    @Admin
    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = DBMS )
    public void activateUser( @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        userManager.activateUser( username, requirePasswordChange );
    }

    @Admin
    @Description( "List all native users." )
    @Procedure( name = "dbms.security.listUsers", mode = DBMS )
    public Stream<UserResult> listUsers()
    {
        Set<String> users = userManager.getAllUsernames();
        if ( users.isEmpty() )
        {
            return Stream.of( userResultForSubject() );
        }
        else
        {
            return users.stream().map( this::userResultForName );
        }
    }

    @Admin
    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = DBMS )
    public Stream<RoleResult> listRoles()
    {
        Set<String> roles = userManager.getAllRoleNames();
        return roles.stream().map( this::roleResultForName );
    }

    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = DBMS )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username )
            throws InvalidArgumentsException
    {
        securityContext.assertCredentialsNotExpired();
        return userManager.getRoleNamesForUser( username ).stream().map( StringResult::new );
    }

    @Admin
    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = DBMS )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName )
            throws InvalidArgumentsException
    {
        return userManager.getUsernamesForRole( roleName ).stream().map( StringResult::new );
    }

    @Admin
    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = DBMS )
    public void createRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        userManager.newRole( roleName );
    }

    @Admin
    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = DBMS )
    public void deleteRole( @Name( "roleName" ) String roleName ) throws InvalidArgumentsException, IOException
    {
        userManager.deleteRole( roleName );
    }

    private void setUserPassword( String username, String newPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        userManager.setUserPassword( username, newPassword != null ? UTF8.encode( newPassword ) : null, requirePasswordChange );
        if ( securityContext.subject().hasUsername( username ) )
        {
            securityContext.subject().setPasswordChangeNoLongerRequired();
        }
    }
}
