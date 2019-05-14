/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.security.SetInitialPasswordCommand;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.assertAuthenticationFailsWithTooManyAttempts;
import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;
import static org.neo4j.server.security.auth.BasicSystemGraphRealmTest.clearedPasswordWithSameLengthAs;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

@ExtendWith( TestDirectoryExtension.class )
class BasicSystemGraphRealmIT
{
    private SystemGraphRealmTestHelper.TestDatabaseManager dbManager;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        dbManager = new SystemGraphRealmTestHelper.TestDatabaseManager( testDirectory );
    }

    @AfterEach
    void tearDown()
    {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldCreateDefaultUserIfNoneExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder(), dbManager );

        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserIfNoneExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().initialUser( "123", false ), dbManager );

        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password("123") ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable
    {
        // Given
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory );

        // Then
        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j1") ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory );

        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );

        realm.stop();

        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j1") ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given started and stopped database
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( dbManager, testDirectory );
        realm.setUserPassword( INITIAL_USER_NAME, UTF8.encode( "neo4j2" ), false );
        realm.stop();
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertFalse( user.credentials().matchesPassword( password("neo4j1") ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j2") ) );
    }

    @Test
    void shouldNotAddInitialUserIfUsersExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm(
                new BasicImportOptionsBuilder().initialUser( "123", false ).migrateUser( "oldUser", "321", false ), dbManager );

        final User initUser = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNull( initUser );

        final User oldUser = realm.silentlyGetUser( "oldUser" );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( password( "321" ) ) );
        assertFalse( oldUser.passwordChangeRequired() );
    }

    @Test
    void shouldNotUpdateUserIfInitialUserExist() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm(
                new BasicImportOptionsBuilder().initialUser( "newPassword", false ).migrateUser( INITIAL_USER_NAME, "oldPassword", true ), dbManager );

        final User oldUser = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( oldUser );
        assertTrue( oldUser.credentials().matchesPassword( password( "oldPassword" ) ) );
        assertTrue( oldUser.passwordChangeRequired() );
    }

    @Test
    void shouldRateLimitAuthentication() throws Throwable
    {
        int maxFailedAttempts = Config.defaults().get( GraphDatabaseSettings.auth_max_failed_attempts );
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" ), dbManager );

        // First make sure one of the users will have a cached successful authentication result for variation
        assertAuthenticationSucceeds( realm, "alice" );

        assertAuthenticationFailsWithTooManyAttempts( realm, "alice", maxFailedAttempts + 1 );
        assertAuthenticationFailsWithTooManyAttempts( realm, "bob", maxFailedAttempts + 1 );
    }

    @Test
    void shouldClearPasswordOnNewUser() throws Throwable
    {
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" ), dbManager );

        byte[] password = password( "jake" );

        // When
        realm.newUser( "jake", password, true );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnNewUserAlreadyExists() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" ), dbManager );

        realm.newUser( "jake", password( "jake" ), true );
        byte[] password = password( "abc123" );

        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.newUser( "jake", password, true ) );
        assertThat( exception.getMessage(), equalTo( "The specified user 'jake' already exists." ) );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLengthAs( "abc123" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPassword() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" ), dbManager );

        realm.newUser( "jake", password( "abc123" ), false );

        byte[] newPassword = password( "jake" );

        // When
        realm.setUserPassword( "jake", newPassword, false );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPasswordWithInvalidPassword() throws Throwable
    {
        // Given
        BasicSystemGraphRealm realm = TestBasicSystemGraphRealm.testRealm( new BasicImportOptionsBuilder().migrateUsers( "alice", "bob" ), dbManager );

        realm.newUser( "jake", password( "jake" ), false );
        byte[] newPassword = password( "jake" );

        // When
        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.setUserPassword( "jake", newPassword, false ) );
        assertThat( exception.getMessage(), equalTo( "Old password and new password cannot be the same." ) );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLengthAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    static void simulateSetInitialPasswordCommand( TestDirectory testDirectory ) throws IncorrectUsage, CommandFailed
    {
        OutsideWorld mock = mock( OutsideWorld.class );
        when( mock.fileSystem() ).thenReturn( testDirectory.getFileSystem() );
        SetInitialPasswordCommand setPasswordCommand =
                new SetInitialPasswordCommand( testDirectory.directory().toPath(), testDirectory.directory( "conf" ).toPath(), mock );
        setPasswordCommand.execute( new String[]{"neo4j1"} );
    }
}
