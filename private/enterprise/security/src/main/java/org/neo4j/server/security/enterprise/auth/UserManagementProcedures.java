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
import org.neo4j.string.UTF8;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( "UnusedReturnValue" )
public class UserManagementProcedures extends AuthProceduresBase
{

    @Admin
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        // TODO: Deprecate this and create a new procedure that takes password as a byte[]
        userManager.newUser( username, password != null ? UTF8.encode( password ) : null, requirePasswordChange );
    }

    @Deprecated
    @Description( "Change the current user's password. Deprecated by dbms.security.changePassword." )
    @Procedure( name = "dbms.changePassword", mode = DBMS, deprecatedBy = "dbms.security.changePassword" )
    public void changePasswordDeprecated( @Name( "password" ) String password )
            throws InvalidArgumentsException, IOException
    {
        // TODO: Deprecate this and create a new procedure that takes password as a byte[]
        changePassword( password, false );
    }

    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = DBMS )
    public void changePassword( @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "false" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        // TODO: Deprecate this and create a new procedure that takes password as a byte[]
        setUserPassword( securityContext.subject().username(), password, requirePasswordChange );
    }

    @Description( "Change the given user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = DBMS )
    public void changeUserPassword( @Name( "username" ) String username, @Name( "newPassword" ) String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        // TODO: Deprecate this and create a new procedure that takes password as a byte[]
        securityContext.assertCredentialsNotExpired();
        setUserPassword( username, newPassword, requirePasswordChange );
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
