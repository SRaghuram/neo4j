/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

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

    @BeforeEach
    void setup()
    {
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
        EnterpriseUserManager realm = mock( EnterpriseUserManager.class );
        evilUserManager = new EvilUserManager( realm );
    }

    @Test
    void shouldLogFailureCreateUser()
    {
        // Given
        PersonalUserManager userManager = getUserManager( "Alice", true );
        evilUserManager.setFailNextCall();
        log.clear();

        //Expect
        assertException( () -> userManager.newUser( "HeWhoShallNotBeNamed", password( "avada kedavra" ), false ), InvalidArgumentsException.class );
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

    private PersonalUserManager getUserManager( String userName, boolean isAdmin )
    {
        return new PersonalUserManager( evilUserManager, new MockAuthSubject( userName ), securityLog, isAdmin );
    }

    private AssertableLogProvider.LogMatcher error( String message, Object... arguments )
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
                throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "newUserException" );
            }
            return delegate.newUser( username, password, changeRequired );
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
                throws InvalidArgumentsException
        {
            if ( failNextCall )
            {
                failNextCall = false;
                throw new InvalidArgumentsException( "setUserPasswordException" );
            }
            delegate.setUserPassword( username, password, requirePasswordChange );
        }

        @Override
        public Set<ResourcePrivilege> getPrivilegesForRoles( Set<String> roles )
        {
            return delegate.getPrivilegesForRoles( roles );
        }

        @Override
        public void clearCacheForRoles()
        {
            delegate.clearCacheForRoles();
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
