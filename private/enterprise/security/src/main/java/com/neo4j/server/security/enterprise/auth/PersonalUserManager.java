/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

class PersonalUserManager implements EnterpriseUserManager
{
    private final EnterpriseUserManager userManager;
    private final SecurityLog securityLog;
    private final AuthSubject subject;
    private final boolean isUserManager;

    PersonalUserManager( EnterpriseUserManager userManager, AuthSubject subject, SecurityLog securityLog, boolean isUserManager )
    {
        this.userManager = userManager;
        this.securityLog = securityLog;
        this.subject = subject;
        this.isUserManager = isUserManager;
    }

    @Override
    public User newUser( String username, byte[] initialPassword, boolean requirePasswordChange )
            throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            User user = userManager.newUser( username, initialPassword, requirePasswordChange );
            securityLog.info( subject, "created user `%s`%s", username,
                    requirePasswordChange ? ", with password change required" : "" );
            return user;
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to create user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteUser( String username ) throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            if ( subject.hasUsername( username ) )
            {
                throw new InvalidArgumentsException( "Deleting yourself (user '" + username + "') is not allowed." );
            }
            boolean wasDeleted = userManager.deleteUser( username );
            securityLog.info( subject, "deleted user `%s`", username );
            return wasDeleted;
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to delete user `%s`: %s", username, e.getMessage() );
            throw e;
        }
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        return userManager.getUser( username );
    }

    @Override
    public User silentlyGetUser( String username )
    {
        return userManager.silentlyGetUser( username );
    }

    @Override
    public void newRole( String roleName, String... usernames ) throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            userManager.newRole( roleName, usernames );
            securityLog.info( subject, "created role `%s`", roleName );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to create role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public boolean deleteRole( String roleName ) throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            boolean wasDeleted = userManager.deleteRole( roleName );
            securityLog.info( subject, "deleted role `%s`", roleName );
            return wasDeleted;
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to delete role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public void setUserPassword( String username, byte[] password, boolean requirePasswordChange )
            throws InvalidArgumentsException, AuthorizationViolationException
    {
        if ( subject.hasUsername( username ) )
        {
            try
            {
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( subject, "changed password%s",
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( AuthorizationViolationException | InvalidArgumentsException e )
            {
                securityLog.error( subject, "tried to change password: %s", e.getMessage() );
                throw e;
            }
        }
        else
        {
            try
            {
                assertUserManager();
                userManager.setUserPassword( username, password, requirePasswordChange );
                securityLog.info( subject, "changed password for user `%s`%s", username,
                        requirePasswordChange ? ", with password change required" : "" );
            }
            catch ( AuthorizationViolationException | InvalidArgumentsException e )
            {
                securityLog.error( subject, "tried to change password for user `%s`: %s", username,
                        e.getMessage() );
                throw e;
            }
        }
    }

    @Override
    public Set<String> getAllUsernames() throws AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getAllUsernames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( subject, "tried to list users: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public void assertRoleExists( String roleName ) throws InvalidArgumentsException
    {
        userManager.assertRoleExists( roleName );
    }

    @Override
    public void addRoleToUser( String roleName, String username ) throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            userManager.addRoleToUser( roleName, username );
            securityLog.info( subject, "added role `%s` to user `%s`", roleName, username );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to add role `%s` to user `%s`: %s", roleName, username,
                    e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
    {
        assertUserManager();
        return userManager.getPrivilegesForRoles( roles );
    }

    @Override
    public void clearCacheForRoles()
    {
        userManager.clearCacheForRoles();
    }

    @Override
    public Set<String> getAllRoleNames() throws AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getAllRoleNames();
        }
        catch ( AuthorizationViolationException e )
        {
            securityLog.error( subject, "tried to list roles: %s", e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException, AuthorizationViolationException
    {
        try
        {
            assertUserManager();
            return userManager.getUsernamesForRole( roleName );
        }
        catch ( AuthorizationViolationException | InvalidArgumentsException e )
        {
            securityLog.error( subject, "tried to list users for role `%s`: %s", roleName, e.getMessage() );
            throw e;
        }
    }

    @Override
    public Set<String> silentlyGetUsernamesForRole( String roleName )
    {
        return userManager.silentlyGetUsernamesForRole( roleName );
    }

    private void assertSelfOrUserManager( String username )
    {
        if ( !subject.hasUsername( username ) )
        {
            assertUserManager();
        }
    }

    private void assertUserManager() throws AuthorizationViolationException
    {
        if ( !isUserManager )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
    }
}
