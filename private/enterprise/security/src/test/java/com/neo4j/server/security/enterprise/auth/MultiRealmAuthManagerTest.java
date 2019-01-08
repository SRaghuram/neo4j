/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Map;
import java.util.function.ToIntFunction;

import org.neo4j.commandline.admin.security.SetDefaultAdminCommand;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.InitialUserTest;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Strings.escape;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.clearedPasswordWithSameLenghtAs;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.password;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.test.assertion.Assert.assertException;

public class MultiRealmAuthManagerTest extends InitialUserTest
{
    private AuthenticationStrategy authStrategy;
    private MultiRealmAuthManager manager;
    private EnterpriseUserManager userManager;
    private AssertableLogProvider logProvider;

    @Rule
    public ExpectedException expect = ExpectedException.none();

    private final ToIntFunction<String> token = s -> -1;

    @Before
    public void setUp() throws Throwable
    {
        config = Config.defaults();
        users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        authStrategy = mock( AuthenticationStrategy.class );
        logProvider = new AssertableLogProvider();

        manager = createAuthManager( true );
        userManager = manager.getUserManager();
    }

    private MultiRealmAuthManager createAuthManager( boolean logSuccessfulAuthentications ) throws Throwable
    {
        Log log = logProvider.getLog( this.getClass() );

        InternalFlatFileRealm internalFlatFileRealm =
                new InternalFlatFileRealm(
                        users,
                        new InMemoryRoleRepository(),
                        mock( PasswordPolicy.class ),
                        authStrategy,
                        mock( JobScheduler.class ),
                        CommunitySecurityModule.getInitialUserRepository(
                                config, NullLogProvider.getInstance(), fsRule.get() ),
                        CommercialSecurityModule.getDefaultAdminRepository(
                                config, NullLogProvider.getInstance(), fsRule.get() )
                    );

        manager = new MultiRealmAuthManager( internalFlatFileRealm, Collections.singleton( internalFlatFileRealm ),
                new MemoryConstrainedCacheManager(), new SecurityLog( log ), logSuccessfulAuthentications,
                false, Collections.emptyMap() );

        manager.init();
        return manager;
    }

    @After
    public void tearDown() throws Throwable
    {
        manager.stop();
        manager.shutdown();
    }

    @Test
    public void shouldMakeOnlyUserAdminIfNoRolesFile() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , false ) );

        // When
        manager.start();

        // Then
        assertThat( manager.getUserManager().getRoleNamesForUser( "jake" ), contains( PredefinedRoles.ADMIN ) );
    }

    @Test
    public void shouldMakeNeo4jUserAdminIfNoRolesFileButManyUsers() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , false ) );
        users.create( newUser( "neo4j", "neo4j" , false ) );

        // When
        manager.start();

        // Then
        assertThat( manager.getUserManager().getRoleNamesForUser( "neo4j" ), contains( PredefinedRoles.ADMIN ) );
        assertThat( manager.getUserManager().getRoleNamesForUser( "jake" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldFailIfNoRolesFileButManyUsersAndNoDefaultAdminOrNeo4j() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , false ) );
        users.create( newUser( "jane", "123abc" , false ) );

        expect.expect( InvalidArgumentsException.class );
        expect.expectMessage( "No roles defined, and cannot determine which user should be admin. " +
                              "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME + "` to select an admin." );

        manager.start();
    }

    @Test
    public void shouldFailIfNoRolesFileButManyUsersAndNonExistingDefaultAdmin() throws Throwable
    {
        // Given
        UserRepository defaultAdminRepository =
                CommercialSecurityModule.getDefaultAdminRepository( config, NullLogProvider.getInstance(), fsRule.get() );
        defaultAdminRepository.start();
        defaultAdminRepository.create(
                new User.Builder( "foo", LegacyCredential.INACCESSIBLE ).withRequiredPasswordChange( false ).build() );
        defaultAdminRepository.shutdown();

        users.create( newUser( "jake", "abc123" , false ) );
        users.create( newUser( "jane", "123abc" , false ) );

        expect.expect( InvalidArgumentsException.class );
        expect.expectMessage( "No roles defined, and default admin user 'foo' does not exist. " +
                              "Please use `neo4j-admin " + SetDefaultAdminCommand.COMMAND_NAME + "` to select a valid admin." );

        manager.start();
    }

    @Test
    public void shouldFindAndAuthenticateUserSuccessfully() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , false ) );
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
    public void shouldNotLogAuthenticationIfFlagSaysNo() throws Throwable
    {
        // Given
        manager.shutdown();
        manager = createAuthManager( false );

        users.create( newUser( "jake", "abc123" , false ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
        logProvider.assertNone( info( "[jake]: logged in" ) );
    }

    @Test
    public void shouldReturnTooManyAttemptsWhenThatIsAppropriate() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , true ) );
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
    public void shouldFindAndAuthenticateUserAndReturnPasswordChangeIfRequired() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123" , true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // When
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) );
        logProvider.assertExactly( info( "[jake]: logged in (password change required)" ) );
    }

    @Test
    public void shouldFailWhenAuthTokenIsInvalid() throws Throwable
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
    public void shouldFailAuthenticationIfUserIsNotFound() throws Throwable
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
    public void shouldFailAuthenticationAndEscapeIfUserIsNotFound() throws Throwable
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
    public void shouldCreateUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        userManager.newUser( "foo", password( "bar" ), true );

        // Then
        User user = users.getUserByName( "foo" );
        assertNotNull( user );
        assertTrue( user.passwordChangeRequired() );
        assertTrue( user.credentials().matchesPassword( "bar" ) );
    }

    @Test
    public void shouldDeleteUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123" , true );
        final User user2 = newUser( "neo4j", "321cba" , true );
        users.create( user );
        users.create( user2 );
        manager.start();

        // When
        userManager.deleteUser( "jake" );

        // Then
        assertNull( users.getUserByName( "jake" ) );
        assertNotNull( users.getUserByName( "neo4j" ) );
    }

    @Test
    public void shouldFailDeletingUnknownUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123" , true );
        users.create( user );
        manager.start();

        // When
        assertException( () -> userManager.deleteUser( "unknown" ),
                InvalidArgumentsException.class, "User 'unknown' does not exist" );

        // Then
        assertNotNull( users.getUserByName( "jake" ) );
    }

    @Test
    public void shouldSuspendExistingUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123" , true );
        users.create( user );
        manager.start();

        // When
        userManager.suspendUser( "jake" );

        // Then
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldActivateExistingUser() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123", false ) );
        manager.start();

        userManager.suspendUser( "jake" );

        // When
        userManager.activateUser( "jake", false );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldSuspendSuspendedUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123", false );
        users.create( user );
        manager.start();
        userManager.suspendUser( "jake" );

        // When
        userManager.suspendUser( "jake" );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldActivateActiveUser() throws Throwable
    {
        // Given
        final User user = newUser( "jake", "abc123", false );
        users.create( user );
        manager.start();
        when( authStrategy.authenticate( user,  password( "abc123" ) ) ).thenReturn( AuthenticationResult.SUCCESS );

        // When
        userManager.activateUser( "jake", false );
        setMockAuthenticationStrategyResult( "jake", "abc123", AuthenticationResult.SUCCESS );

        // Then
        AuthenticationResult result = manager.login( authToken( "jake", "abc123" ) ).subject().getAuthenticationResult();
        assertThat( result, equalTo( AuthenticationResult.SUCCESS ) );
    }

    @Test
    public void shouldFailToSuspendNonExistingUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.suspendUser( "jake" );
            fail( "Should throw exception on suspending unknown user" );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat(e.getMessage(), containsString("User 'jake' does not exist"));
        }
    }

    @Test
    public void shouldFailToActivateNonExistingUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.activateUser( "jake", false );
            fail( "Should throw exception on activating unknown user" );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat(e.getMessage(), containsString("User 'jake' does not exist"));
        }
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        // Given
        users.create( newUser( "jake", "abc123", true ) );
        manager.start();

        // When
        userManager.setUserPassword( "jake", password( "hello, world!" ), false );

        // Then
        User user = userManager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "hello, world!" ) );
        assertThat( users.getUserByName( "jake" ), equalTo( user ) );
    }

    @Test
    public void shouldNotRequestPasswordChangeWithInvalidCredentials() throws Throwable
    {
        // Given
        users.create( newUser( "neo", "abc123", true ) );
        manager.start();
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );
        setMockAuthenticationStrategyResult( "neo", "wrong", AuthenticationResult.FAILURE );

        // When
        AuthenticationResult result = manager.login( authToken( "neo", "wrong" ) ).subject().getAuthenticationResult();

        // Then
        assertThat( result, equalTo( AuthenticationResult.FAILURE ) );
    }

    @Test
    public void shouldReturnNullWhenSettingPasswordForUnknownUser() throws Throwable
    {
        // Given
        manager.start();

        // When
        try
        {
            userManager.setUserPassword( "unknown", password( "hello, world!" ), false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }
    }

    private void createTestUsers() throws Throwable
    {
        userManager.newUser( "morpheus", password( "abc123" ), false );
        userManager.newRole( "admin", "morpheus" );
        setMockAuthenticationStrategyResult( "morpheus", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "trinity", password( "abc123" ), false );
        userManager.newRole( "architect", "trinity" );
        setMockAuthenticationStrategyResult( "trinity", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "tank", password( "abc123" ), false );
        userManager.newRole( "publisher", "tank" );
        setMockAuthenticationStrategyResult( "tank", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "neo", password( "abc123" ), false );
        userManager.newRole( "reader", "neo" );
        setMockAuthenticationStrategyResult( "neo", "abc123", AuthenticationResult.SUCCESS );

        userManager.newUser( "smith", password( "abc123" ), false );
        userManager.newRole( "agent", "smith" );
        setMockAuthenticationStrategyResult( "smith", "abc123", AuthenticationResult.SUCCESS );
    }

    @Test
    public void defaultUserShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        manager.start();
        setMockAuthenticationStrategyResult( "neo4j", "neo4j", AuthenticationResult.SUCCESS );

        // When
        SecurityContext securityContext = manager.login( authToken( "neo4j", "neo4j" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        userManager.setUserPassword( "neo4j", password( "1234" ), false );
        securityContext.subject().logout();

        setMockAuthenticationStrategyResult( "neo4j", "1234", AuthenticationResult.SUCCESS );
        securityContext = manager.login( authToken( "neo4j", "1234" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void userWithAdminRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        SecurityContext securityContext = manager.login( authToken( "morpheus", "abc123" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void userWithArchitectRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        SecurityContext securityContext = manager.login( authToken( "trinity", "abc123" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void userWithPublisherRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        SecurityContext securityContext = manager.login( authToken( "tank", "abc123" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( "should allow reads", securityContext.mode().allowsReads() );
        assertTrue( "should allow writes", securityContext.mode().allowsWrites() );
        assertFalse( "should _not_ allow schema writes", securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void userWithReaderRoleShouldHaveCorrectPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        SecurityContext securityContext = manager.login( authToken( "neo", "abc123" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertTrue( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void userWithNonPredefinedRoleShouldHaveNoPermissions() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        SecurityContext securityContext = manager.login( authToken( "smith", "abc123" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // Then
        assertFalse( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void shouldHaveNoPermissionsAfterLogout() throws Throwable
    {
        // Given
        createTestUsers();
        manager.start();

        // When
        LoginContext loginContext = manager.login( authToken( "morpheus", "abc123" ) );
        SecurityContext securityContext = loginContext.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        assertTrue( securityContext.mode().allowsReads() );
        assertTrue( securityContext.mode().allowsWrites() );
        assertTrue( securityContext.mode().allowsSchemaWrites() );

        loginContext.subject().logout();

        securityContext = loginContext.authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        // Then
        assertFalse( securityContext.mode().allowsReads() );
        assertFalse( securityContext.mode().allowsWrites() );
        assertFalse( securityContext.mode().allowsSchemaWrites() );
    }

    @Test
    public void shouldClearPasswordOnLogin() throws Throwable
    {
        // Given
        when( authStrategy.authenticate( any(), any() ) ).thenReturn( AuthenticationResult.SUCCESS );

        manager.start();
        userManager.newUser( "jake", password( "abc123" ), true );
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );

        // When
        manager.login( authToken );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
    }

    @Test
    public void shouldClearPasswordOnInvalidAuthToken() throws Throwable
    {
        // Given
        manager.start();
        byte[] password = password( "abc123" );
        Map<String,Object> authToken = AuthToken.newBasicAuthToken( "jake", password );
        authToken.put( AuthToken.SCHEME_KEY, null ); // Null is not a valid scheme

        // When
        try
        {
            manager.login( authToken );
            fail( "exception expected" );
        }
        catch ( InvalidAuthTokenException e )
        {
            // expected
        }
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
        assertThat( authToken.get( AuthToken.CREDENTIALS ), equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
    }

    @Test
    public void shouldClearPasswordOnNewUser() throws Throwable
    {
        // Given
        manager.start();
        byte[] password = password( "abc123" );

        // When
        userManager.newUser( "jake", password, true );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
        User user = userManager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "abc123" ) );
    }

    @Test
    public void shouldClearPasswordOnNewUserAlreadyExists() throws Throwable
    {
        // Given
        manager.start();
        userManager.newUser( "jake", password( "abc123" ), true );
        byte[] password = password( "abc123" );

        // When
        try
        {
            userManager.newUser( "jake", password, true );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
    }

    @Test
    public void shouldClearPasswordOnSetUserPassword() throws Throwable
    {
        // Given
        manager.start();
        userManager.newUser( "jake", password( "old" ), false );
        byte[] newPassword = password( "abc123" );

        // When
        userManager.setUserPassword( "jake", newPassword, false );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
        User user = userManager.getUser( "jake" );
        assertTrue( user.credentials().matchesPassword( "abc123" ) );
    }

    @Test
    public void shouldClearPasswordOnSetUserPasswordWithInvalidPassword() throws Throwable
    {
        // Given
        manager.start();
        userManager.newUser( "jake", password( "abc123" ), false );
        byte[] newPassword = password( "abc123" );

        // When
        try
        {
            userManager.setUserPassword( "jake", newPassword, false );
            fail( "exception expected" );
        }
        catch ( InvalidArgumentsException e )
        {
            // expected
        }

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
    }

    private AssertableLogProvider.LogMatcher info( String message )
    {
        return inLog( this.getClass() ).info( message );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private void setMockAuthenticationStrategyResult( String username, String password, AuthenticationResult result )
    {
        final User user = users.getUserByName( username );
        when( authStrategy.authenticate( user, password( password ) ) ).thenReturn( result );
    }

    @Override
    protected AuthManager authManager()
    {
        return manager;
    }
}
