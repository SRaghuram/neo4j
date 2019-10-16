/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "unused" )
public class RoleManagementProcedures extends AuthProceduresBase
{
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
}
