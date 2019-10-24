/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.InMemoryUserManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.escape;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.BasicSystemGraphRealmTest.clearedPasswordWithSameLengthAs;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.test.assertion.Assert.assertException;

class MultiRealmAuthManagerTest
{
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private AssertableLogProvider logProvider;
    private InMemoryUserManager realm;

    @BeforeEach
    void setUp() throws Throwable
    {
        authStrategy = mock( AuthenticationStrategy.class );
        logProvider = new AssertableLogProvider();

        manager = createAuthManager( true );
    }

    private MultiRealmAuthManager createAuthManager( boolean logSuccessfulAuthentications ) throws Throwable
    {
        realm = new InMemoryUserManager( Config.defaults(), authStrategy );

        manager = new MultiRealmAuthManager( realm, Collections.singleton( realm ),
                new MemoryConstrainedCacheManager(), new SecurityLog( logProvider.getLog( this.getClass() ) ), logSuccessfulAuthentications );

        manager.init();
        return manager;
    }

    @AfterEach
    void tearDown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    @Test
    void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        realm.newUser( "jake", password( "abc123" ), false );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject()
                .getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertExactly( info( "[jake]: logged in" ) );
    }

    @Test
    void shouldNotLogAuthenticationIfFlagSaysNo() throws Throwable
    {
        // Given
        manager.shutdown();
        manager = createAuthManager( false );

        realm.newUser( "jake", password( "abc123" ), false );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertNone( info( "[jake]: logged in" ) );
    }

    @Test
    void shouldReturnTooManyAttemptsWhenThatIsAppropriate() throws Throwable
    {
        // Given
        realm.newUser( "jake", password( "abc123" ), true );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "wrong password", AuthenticationResult.TOO_MANY_ATTEMPTS );

        // When
        AuthSubject authSubject = manager.login( authToken( "jake", "wrong password" ) ).subject();
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.TOO_MANY_ATTEMPTS ) );
        logProvider.assertExactly(
                error( "[%s]: failed to log in: too many failed attempts", "jake" ) );
    }

    @Test
    void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        realm.newUser( "jake", password( "abc123" ), true );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
        logProvider.assertExactly( info( "[jake]: logged in (password change required)" ) );
    }

    @Test
    void shouldFailWhenAuthTokenIsInvalid() throws Throwable
    {
        manager.start();

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "supercool", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token: { scheme='supercool', principal='neo4j' }" );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "none" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, scheme='none' only allowed when auth is disabled: { scheme='none' }" );

        assertException(
                () -> manager.login( map( "key", "value" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `scheme`: { key='value' }" );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.PRINCIPAL, "neo4j" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `credentials`: { scheme='basic', principal='neo4j' }" );

        assertException(
                () -> manager.login( map( AuthToken.SCHEME_KEY, "basic", AuthToken.CREDENTIALS, "very-secret" ) ),
                InvalidAuthTokenException.class,
                "Unsupported authentication token, missing key `principal`: { scheme='basic', credentials='******' }" );
    }

    @Test
    void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.start();

        // When
        AuthSubject authSubject = manager.login( authToken( "unknown", "abc123" ) ).subject();
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
        logProvider.assertExactly( error( "[%s]: failed to log in: %s", "unknown", "invalid principal or credentials" ) );
    }

    @Test
    void shouldFailAuthenticationAndEscapeIfUserIsNotFound() throws Throwable
    {
        // Given
        manager.start();

        // When
        AuthSubject authSubject = manager.login( authToken( "unknown\n\t\r\"haxx0r\"", "abc123" ) ).subject();
        AuthenticationResult result = authSubject.getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
        logProvider.assertExactly( error( "[%s]: failed to log in: %s",
                escape( "unknown\n\t\r\"haxx0r\"" ), "invalid principal or credentials" ) );
    }

    @Test
    void shouldNotRequestPasswordChangeWithInvalidCredentials() throws Throwable
    {
        // Given
        realm.newUser( "neo", password( "abc123" ), true );
        manager.start();
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );
        setMockAuthenticationStrategyResult( "neo", "wrong", AuthenticationResult.FAILURE );

        // When
        AuthenticationResult result = manager.login( authToken( "neo", "wrong" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @SuppressWarnings( "Duplicates" )
    @Test
    void shouldClearPasswordOnLogin() throws Throwable
    {
        // Given
        when( authStrategy.authenticate( any(), any() ) ).thenReturn( AuthenticationResult.SUCCESS );

        manager.start();
        realm.newUser( "jake", password( "abc123" ), true );
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );

        // When
        manager.login( authToken );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    @Test
    void shouldClearPasswordOnInvalidAuthToken() throws Throwable
    {
        // Given
        manager.start();
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );
        authToken.put( AuthToken.SCHEME_KEY, null ); // Null is not a valid scheme

        // When
        assertException( () -> manager.login( authToken ), InvalidAuthTokenException.class );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
    }

    private AssertableLogProvider.LogMatcher info( String message )
    {
        return inLog( this.getClass() ).info( message );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private void setMockAuthenticationStrategyResult( String username, String password, AuthenticationResult result ) throws InvalidArgumentsException
    {
        final User user = realm.getUser( username );
        when( authStrategy.authenticate( user, password( password ) ) ).thenReturn( result );
    }
}
