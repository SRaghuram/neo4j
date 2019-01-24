/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.assertion.Assert.assertException;

class PersonalUserManagerTest
{
    private EvilUserManager evilUserManager;
    private AssertableLogProvider log;
    private SecurityLog securityLog;

    private static final String ROLE_NAME = "custom";

    @Test
    void shouldLogFailureCreateUser()
    {
        // Given
        PersonalUserManager userManager = getUserManager( "Alice", true );
        evilUserManager.setFailNextCall();
        log.clear();

        //Expect
        assertException( () -> userManager.newUser( "HeWhoShallNotBeNamed", password( "avada kedavra" ), false ), IOException.class );
        log.assertExactly( error( "[Alice]: tried to create user `%s`: %s", "HeWhoShallNotBeNamed", "newUserException" ) );
    }

    @Test
    void shouldLogUnauthorizedCreateUser()
    {
        // Given
        PersonalUserManager userManager = getUserManager( "Bob", false );
        log.clear();

        //Expect
        assertException( () -> userManager.newUser( "HeWhoShallNotBeNamed", password( "avada kedavra" ), false ), AuthorizationViolationException.class );
        log.assertExactly( error( "[Bob]: tried to create user `%s`: %s", "HeWhoShallNotBeNamed", "Permission denied." ) );
    }

    @Test
    void shouldLogSuccessAssignPrivilege() throws IOException, InvalidArgumentsException
    {
        PersonalUserManager userManager = getUserManager( "Alice", true );
        userManager.newRole( ROLE_NAME );
        log.clear();

        userManager.grantPrivilegeToRole( ROLE_NAME, new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        log.assertExactly( info( "[Alice]: added `%s` privilege on `%s` for role `%s`", "read", "graph", ROLE_NAME ) );
    }

    @Test
    void shouldLogFailureAssignPrivilege()
    {
        PersonalUserManager userManager = getUserManager( "Alice", true );
        log.clear();
        evilUserManager.setFailNextCall();

        catchInvalidArguments( () -> userManager.grantPrivilegeToRole( ROLE_NAME,
                new ResourcePrivilege( Action.READ, Resource.GRAPH ) ) );
        log.assertExactly(
                error( "[Alice]: tried to add `%s` privilege on `%s` for role `%s`: %s", "read", "graph", ROLE_NAME, "assignPrivilegeToRoleException" )
        );
    }

    @Test
    void shouldLogUnauthorizedAssignPrivilege()
    {
        PersonalUserManager userManager = getUserManager( "Bob", false );
        log.clear();

        catchAuthorizationViolation( () -> userManager.grantPrivilegeToRole( ROLE_NAME,
                new ResourcePrivilege( Action.READ, Resource.GRAPH ) ) );
        log.assertExactly(
                error( "[Bob]: tried to add `%s` privilege on `%s` for role `%s`: %s", "read", "graph", ROLE_NAME, "Permission denied." ) );
    }

    @Test
    void shouldLogSuccessSetAdmin() throws IOException, InvalidArgumentsException
    {
        PersonalUserManager userManager = getUserManager( "Alice", true );
        userManager.newRole( ROLE_NAME );
        log.clear();

        userManager.setAdmin( ROLE_NAME, true );
        userManager.setAdmin( ROLE_NAME, false );
        log.assertExactly(
                info( "[Alice]: %s admin privilege for role `%s`", "granted", ROLE_NAME ),
                info( "[Alice]: %s admin privilege for role `%s`", "revoked", ROLE_NAME )
        );
    }

    @Test
    void shouldLogFailureSetAdmin()
    {
        PersonalUserManager userManager = getUserManager( "Alice", true );
        log.clear();

        evilUserManager.setFailNextCall();
        catchInvalidArguments( () -> userManager.setAdmin( ROLE_NAME, true ) );
        evilUserManager.setFailNextCall();
        catchInvalidArguments( () -> userManager.setAdmin( ROLE_NAME, false ) );
        log.assertExactly(
                error( "[Alice]: tried to %s admin privilege for role `%s`: %s", "grant", ROLE_NAME, "setAdminException" ),
                error( "[Alice]: tried to %s admin privilege for role `%s`: %s", "revoke", ROLE_NAME, "setAdminException" )
        );
    }

    @Test
    void shouldLogUnauthorizedShowPrivileges()
    {
        PersonalUserManager userManager = getUserManager( "Bob", false );
        log.clear();

        catchAuthorizationViolation( () -> userManager.showPrivilegesForUser( "Alice" ) );
        log.assertExactly(
                error( "[Bob]: tried to show privileges for user `%s`: %s", "Alice", "Permission denied." ) );
    }

    @Test
    void shouldLogFailureShowPrivileges()
    {
        PersonalUserManager userManager = getUserManager( "Alice", true );
        log.clear();

        evilUserManager.setFailNextCall();
        catchInvalidArguments( () -> userManager.showPrivilegesForUser( "IDoNotExist" ) );
        log.assertExactly(
                error( "[Alice]: tried to show privileges for user `%s`: %s", "IDoNotExist", "showPrivilegesForUserException" ) );
    }

    @BeforeEach
    void setup()
    {
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
        EnterpriseUserManager realm = mock( EnterpriseUserManager.class );
        evilUserManager = new EvilUserManager( realm );
    }

    private PersonalUserManager getUserManager( String userName, boolean isAdmin )
    {
        return new PersonalUserManager( evilUserManager, new MockAuthSubject( userName ), securityLog, isAdmin );
    }

    private void catchInvalidArguments( ThrowingAction<Exception> f )
    {
        assertException( f, InvalidArgumentsException.class );
    }

    private void catchAuthorizationViolation( ThrowingAction<Exception> f )
    {
        assertException( f, AuthorizationViolationException.class );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private class EvilUserManager implements EnterpriseUserManager
    {
        private boolean failNextCall;
        private EnterpriseUserManager delegate;

        EvilUserManager( EnterpriseUserManager delegate )
        {
            this.delegate = delegate;
        }

        void setFailNextCall()
        {
            failNextCall = true;
        }

        @Override
        public User newUser( String username, byte[] password, boolean changeRequired )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "newUserException" );
            }
            return delegate.newUser( username, password, changeRequired );
        }

        @Override
        public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "deleteUserException" );
            }
            return delegate.deleteUser( username );
        }

        @Override
        public User getUser( String username ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getUserException" );
            }
            return delegate.getUser( username );
        }

        @Override
        public User silentlyGetUser( String username )
        {
            return delegate.silentlyGetUser( username );
        }

        @Override
        public void setUserPassword( String username, byte[] password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "setUserPasswordException" );
            }
            delegate.setUserPassword( username, password, requirePasswordChange );
        }

        @Override
        public Set<String> getAllUsernames()
        {
            return delegate.getAllUsernames();
        }

        @Override
        public void suspendUser( String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "suspendUserException" );
            }
            delegate.suspendUser( username );
        }

        @Override
        public void activateUser( String username, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "activateUserException" );
            }
            delegate.activateUser( username, requirePasswordChange );
        }

        @Override
        public void newRole( String roleName, String... usernames ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "newRoleException" );
            }
            delegate.newRole( roleName, usernames );
        }

        @Override
        public boolean deleteRole( String roleName ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "deleteRoleException" );
            }
            return delegate.deleteRole( roleName );
        }

        @Override
        public void assertRoleExists( String roleName ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getRoleException" );
            }
            delegate.assertRoleExists( roleName );
        }

        @Override
        public void addRoleToUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "addRoleToUserException" );
            }
            delegate.addRoleToUser( roleName, username );
        }

        @Override
        public void removeRoleFromUser( String roleName, String username ) throws IOException, InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new IOException( "removeRoleFromUserException" );
            }
            delegate.removeRoleFromUser( roleName, username );
        }

        @Override
        public void grantPrivilegeToRole( String roleName, ResourcePrivilege resourcePrivilege ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "assignPrivilegeToRoleException" );
            }
            delegate.grantPrivilegeToRole( roleName, resourcePrivilege );
        }

        @Override
        public Set<DatabasePrivilege> showPrivilegesForUser( String username ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "showPrivilegesForUserException" );
            }
            return delegate.showPrivilegesForUser( username );
        }

        @Override
        public void setAdmin( String roleName, boolean setToAdmin ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "setAdminException" );
            }
            delegate.setAdmin( roleName, setToAdmin );
        }

        @Override
        public Set<String> getAllRoleNames()
        {
            return delegate.getAllRoleNames();
        }

        @Override
        public Set<String> getRoleNamesForUser( String username ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getRoleNamesForUserException" );
            }
            return delegate.getRoleNamesForUser( username );
        }

        @Override
        public Set<String> silentlyGetRoleNamesForUser( String username )
        {
            return delegate.silentlyGetRoleNamesForUser( username );
        }

        @Override
        public Set<String> getUsernamesForRole( String roleName ) throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "getUsernamesForRoleException" );
            }
            return delegate.getUsernamesForRole( roleName );
        }

        @Override
        public Set<String> silentlyGetUsernamesForRole( String roleName )
        {
            return delegate.silentlyGetUsernamesForRole( roleName );
        }
    }

    private static class MockAuthSubject implements AuthSubject
    {
        private final String name;

        private MockAuthSubject( String name )
        {
            this.name = name;
        }

        @Override
        public void logout()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return name.equals( username );
        }

        @Override
        public String username()
        {
            return name;
        }
    }
}
