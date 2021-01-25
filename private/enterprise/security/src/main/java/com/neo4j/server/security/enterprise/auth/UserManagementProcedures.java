/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Result.ResultVisitor;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.Sensitive;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static java.util.Collections.emptyMap;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.kernel.api.exceptions.Status.Statement.FeatureDeprecationWarning;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@SuppressWarnings( {"unused"} )
public class UserManagementProcedures extends AuthProceduresBase
{
    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = WRITE, deprecatedBy = "Administration command: CREATE USER" )
    public void createUser(
            @Name( "username" ) String username,
            @Name( "password" ) @Sensitive String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws ProcedureException
    {
        var query = String.format( "CREATE USER %s SET PASSWORD '%s' %s", escapeParameter( username ), password == null ? "" : password,
                requirePasswordChange ? "CHANGE REQUIRED" : "CHANGE NOT REQUIRED" );
        runSystemCommand( query, "dbms.security.createUser" );
    }

    @SystemProcedure
    @Deprecated
    @Description( "Change the current user's password." )
    @Procedure( name = "dbms.security.changePassword", mode = WRITE, deprecatedBy = "Administration command: ALTER CURRENT USER SET PASSWORD" )
    public void changePassword(
            @Name( "password" ) @Sensitive String password,
            @Name( value = "requirePasswordChange", defaultValue = "false" ) boolean requirePasswordChange )
            throws ProcedureException
    {
        throw new ProcedureException( FeatureDeprecationWarning, "This procedure is no longer available, use: 'ALTER CURRENT USER SET PASSWORD'" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Change the given user's password." )
    @Procedure( name = "dbms.security.changeUserPassword", mode = WRITE, deprecatedBy = "Administration command: ALTER USER" )
    public void changeUserPassword(
            @Name( "username" ) String username,
            @Name( "newPassword" ) @Sensitive String newPassword,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws ProcedureException
    {
        var query = String.format( "ALTER USER %s SET PASSWORD '%s' %s", escapeParameter( username ), newPassword == null ? "" : newPassword,
                requirePasswordChange ? "CHANGE REQUIRED" : "CHANGE NOT REQUIRED" );
        runSystemCommand( query, "dbms.security.changeUserPassword" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Assign a role to the user." )
    @Procedure( name = "dbms.security.addRoleToUser", mode = WRITE, deprecatedBy = "Administration command: GRANT ROLE TO USER" )
    public void addRoleToUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username ) throws ProcedureException
    {
        var query = String.format( "GRANT ROLE %s TO %s", escapeParameter( roleName ), escapeParameter( username ) );
        runSystemCommand( query, "dbms.security.addRoleToUser" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Unassign a role from the user." )
    @Procedure( name = "dbms.security.removeRoleFromUser", mode = WRITE, deprecatedBy = "Administration command: REVOKE ROLE FROM USER" )
    public void removeRoleFromUser( @Name( "roleName" ) String roleName, @Name( "username" ) String username ) throws ProcedureException
    {
        var query = String.format( "REVOKE ROLE %s FROM %s", escapeParameter( roleName ), escapeParameter( username ) );
        runSystemCommand( query, "dbms.security.removeRoleFromUser" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Delete the specified user." )
    @Procedure( name = "dbms.security.deleteUser", mode = WRITE, deprecatedBy = "Administration command: DROP USER" )
    public void deleteUser( @Name( "username" ) String username ) throws ProcedureException
    {
        var query = String.format( "DROP USER %s", escapeParameter( username ) );
        runSystemCommand( query, "dbms.security.deleteUser" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Suspend the specified user." )
    @Procedure( name = "dbms.security.suspendUser", mode = WRITE, deprecatedBy = "Administration command: ALTER USER" )
    public void suspendUser( @Name( "username" ) String username ) throws ProcedureException
    {
        var query = String.format( "ALTER USER %s SET STATUS SUSPENDED", escapeParameter( username ) );
        runSystemCommand( query, "dbms.security.suspendUser" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Activate a suspended user." )
    @Procedure( name = "dbms.security.activateUser", mode = WRITE, deprecatedBy = "Administration command: ALTER USER" )
    public void activateUser( @Name( "username" ) String username,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws ProcedureException
    {
        var query = String.format( "ALTER USER %s %sSET STATUS ACTIVE", escapeParameter( username ),
                requirePasswordChange ? "SET PASSWORD CHANGE REQUIRED " : "" );
        runSystemCommand( query, "dbms.security.activateUser" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @SuppressWarnings( "unchecked" )
    @Description( "List all native users." )
    @Procedure( name = "dbms.security.listUsers", mode = READ, deprecatedBy = "Administration command: SHOW USERS" )
    public Stream<UserResult> listUsers() throws ProcedureException
    {
        var result = new ArrayList<UserResult>();
        var visitor = new ResultVisitor<RuntimeException>()
        {
            @Override
            public boolean visit( Result.ResultRow row ) throws RuntimeException
            {
                var user = row.getString( "user" );
                var roles = (List<String>) row.get( "roles" );
                var changeRequired = row.getBoolean( "passwordChangeRequired" );
                var suspended = row.getBoolean( "suspended" );
                result.add( new UserResult( user, roles, changeRequired, suspended ) );
                return true;
            }
        };
        runSystemCommandWithVisitor( "SHOW USERS", emptyMap(), "dbms.security.listUsers", visitor  );

        if ( result.isEmpty() )
        {
            return Stream.of( userResultForSubject() );
        }
        return result.stream();
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "List all available roles." )
    @Procedure( name = "dbms.security.listRoles", mode = READ, deprecatedBy = "Administration command: SHOW ROLES" )
    public Stream<RoleResult> listRoles() throws ProcedureException
    {
        var result = new HashMap<String,Set<String>>();
        var visitor = new ResultVisitor<RuntimeException>()
        {
            @Override
            public boolean visit( Result.ResultRow row ) throws RuntimeException
            {
                var role = row.getString( "role" );
                var user = row.getString( "member" );
                var users = result.computeIfAbsent( role, k -> new HashSet<>() );
                if ( user != null )
                {
                    users.add( user );
                }
                return true;
            }
        };
        runSystemCommandWithVisitor( "SHOW ALL ROLES WITH USERS", emptyMap(), "dbms.security.listRoles", visitor );
        return result.entrySet().stream().map( e -> new RoleResult( e.getKey(), e.getValue() ) );
    }

    @SystemProcedure
    @Deprecated
    @Description( "List all roles assigned to the specified user." )
    @Procedure( name = "dbms.security.listRolesForUser", mode = READ, deprecatedBy = "Administration command: SHOW USERS" )
    public Stream<StringResult> listRolesForUser( @Name( "username" ) String username ) throws ProcedureException, InvalidArgumentsException
    {
        var userExists = new AtomicBoolean( false );
        var result = new HashSet<StringResult>();
        String query = "SHOW ALL ROLES WITH USERS WHERE member = $username";
        Map<String, Object> parameters = java.util.Map.of( "username", username );
        var visitor = new ResultVisitor<RuntimeException>()
        {
            @Override
            public boolean visit( Result.ResultRow row ) throws RuntimeException
            {
                // If the user exists, it will always occur in at least one row due to the PUBLIC role
                userExists.set(true);
                var role = row.getString( "role" );
                result.add( new StringResult( role ) );
                return true;
            }
        };
        runSystemCommandWithVisitor( query, parameters, "dbms.security.listRolesForUser", visitor );

        if ( !userExists.get() )
        {
            throw new InvalidArgumentsException( String.format( "User '%s' does not exist.", username ) );
        }

        return result.stream();
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "List all users currently assigned the specified role." )
    @Procedure( name = "dbms.security.listUsersForRole", mode = READ, deprecatedBy = "Administration command: SHOW ROLES WITH USERS" )
    public Stream<StringResult> listUsersForRole( @Name( "roleName" ) String roleName ) throws ProcedureException, InvalidArgumentsException
    {
        var roleExists = new AtomicBoolean( false );
        var result = new HashSet<StringResult>();
        String query = "SHOW ALL ROLES WITH USERS WHERE role = $roleName";
        Map<String, Object> parameters = java.util.Map.of( "roleName", roleName );
        var visitor = new ResultVisitor<RuntimeException>()
        {
            @Override
            public boolean visit( Result.ResultRow row ) throws RuntimeException
            {
                roleExists.set( true );
                var user = row.getString( "member" );
                if ( user != null )
                {
                    result.add( new StringResult( user ) );
                }
                return true;
            }
        };
        runSystemCommandWithVisitor( query, parameters, "dbms.security.listUsersForRole", visitor );

        if ( !roleExists.get() )
        {
            throw new InvalidArgumentsException( String.format( "Role '%s' does not exist.", roleName ) );
        }

        return result.stream();
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Create a new role." )
    @Procedure( name = "dbms.security.createRole", mode = WRITE, deprecatedBy = "Administration command: CREATE ROLE" )
    public void createRole( @Name( "roleName" ) String roleName ) throws ProcedureException
    {
        var query = String.format( "CREATE ROLE %s", escapeParameter( roleName ) );
        runSystemCommand( query, "dbms.security.createRole" );
    }

    @Admin
    @SystemProcedure
    @Deprecated
    @Description( "Delete the specified role. Any role assignments will be removed." )
    @Procedure( name = "dbms.security.deleteRole", mode = WRITE, deprecatedBy = "Administration command: DROP ROLE" )
    public void deleteRole( @Name( "roleName" ) String roleName ) throws ProcedureException
    {
        var query = String.format( "DROP ROLE %s", escapeParameter( roleName ) );
        runSystemCommand( query, "dbms.security.deleteRole" );
    }

    private boolean isSelf( String username )
    {
        return securityContext.subject().hasUsername( username );
    }

    private HashSet<String> getStrings( Object rolesObj ) throws ProcedureException
    {
        var roles = new HashSet<String>();
        if ( !(rolesObj instanceof Collection) )
        {
            throw new ProcedureException( null, "" );
        }
        for ( var roleObject : (Collection) rolesObj )
        {
            if ( !(roleObject instanceof String) )
            {
                throw new ProcedureException( null, "" );
            }
            roles.add( (String) roleObject );
        }
        return roles;
    }

    private void runSystemCommand( String query, String procedureName ) throws ProcedureException
    {
        try
        {
            Result execute = transaction.execute( query );
            execute.accept( row -> true );
        }
        catch ( Exception e )
        {
            translateException( e, procedureName );
        }
    }

    private void runSystemCommandWithVisitor( String query, Map<String,Object> parameters, String procedureName, ResultVisitor<RuntimeException> visitor )
            throws ProcedureException
    {
        try
        {
            Result execute = transaction.execute( query, parameters );
            execute.accept( visitor );
        }
        catch ( Exception e )
        {
            translateException( e, procedureName );
        }
    }

    private void translateException( Exception e, String procedureName ) throws ProcedureException
    {
        Status status = Status.statusCodeOf( e );
        if ( status != null && status.equals( Status.Statement.NotSystemDatabaseError ) )
        {
            throw new ProcedureException( ProcedureCallFailed, e,
                    String.format( "This is an administration command and it should be executed against the system database: %s", procedureName ) );
        }
        throw new ProcedureException( ProcedureCallFailed, e, e.getMessage() );
    }

    private String escapeParameter( String input )
    {
        return String.format( "`%s`", input == null ? "" : input );
    }
}
