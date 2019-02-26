/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.server.security.enterprise.CommercialSecurityModule;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.commandline.admin.security.ImportAuthCommand.COMMAND_NAME;
import static com.neo4j.commandline.admin.security.ImportAuthCommand.IMPORT_SYSTEM_DATABASE_NAME;
import static com.neo4j.commandline.admin.security.ImportAuthCommand.OFFLINE_ARG_NAME;
import static com.neo4j.commandline.admin.security.ImportAuthCommand.ROLE_ARG_NAME;
import static com.neo4j.commandline.admin.security.ImportAuthCommand.USER_ARG_NAME;
import static com.neo4j.server.security.enterprise.CommercialSecurityModule.ROLE_STORE_FILENAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.CommunitySecurityModule.USER_STORE_FILENAME;

public class ImportAuthCommandIT
{
    private static final String ALTERNATIVE_USER_STORE_FILENAME = "users_to_import";
    private static final String ALTERNATIVE_ROLE_STORE_FILENAME = "roles_to_import";

    private final DefaultFileSystemRule fileSystem = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystem );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystem );

    private File confDir;
    private File homeDir;
    private OutsideWorld out;
    private AdminTool tool;

    @Before
    public void setup() throws IOException
    {
        File graphDir = testDirectory.directory();
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
        fileSystem.mkdirs( confDir );
        fileSystem.mkdirs( homeDir );
        fileSystem.create( new File( confDir, Config.DEFAULT_CONFIG_FILE_NAME ) ).close();
        out = mock( OutsideWorld.class );
        resetOutsideWorldMock();
        resetSystemDatabaseDirectory();
        tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
    }

    @Test
    public void shouldImportAuthFromDefaultUserAndRoleStoreFiles() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME );

        // Then
        assertUserImportFileContainsOnly( "jane", "alice", "bob", "jim" );
        assertRoleImportFileContains( "flunky","jane" );
        assertRoleImportFileContains( "goon","bob", "jim" );
        assertRoleImportFileContains( "taskmaster","alice" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldImportAuthWithGivenUserStoreFile() throws Throwable
    {
        // Given
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME );

        // Then
        assertUserImportFileContainsOnly( "jane", "alice", "bob", "jim" );
        assertRoleImportFileContains( "flunky", "jane" );
        assertRoleImportFileContains( "goon", "bob", "jim" );
        assertRoleImportFileContains( "taskmaster", "alice" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldImportAuthWithGivenRoleStoreFile() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "alice", "bob" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "duct_taper", "alice" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "bob" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContainsOnly( "alice", "bob" );
        assertRoleImportFileContains( "duct_taper", "alice" );
        assertRoleImportFileContains( "box_ticker","bob" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldImportAuthWithGivenUserAndRoleStoreFiles() throws Throwable
    {
        // Given
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "alice", "bob" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "duct_taper", "alice" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "bob" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContainsOnly( "alice", "bob" );
        assertRoleImportFileContains( "duct_taper", "alice" );
        assertRoleImportFileContains( "box_ticker","bob" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldOverwriteImportFiles() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jim", "john" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jim" );
        insertRole( ROLE_STORE_FILENAME, "goon", "john" );

        assertNoUserImportFile();
        assertNoRoleImportFile();

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME );

        // Then
        assertUserImportFileContainsOnly( "jim", "john" );
        assertRoleImportFileContains( "flunky","jim" );
        assertRoleImportFileContains( "goon","john" );
        assertSuccessfulOutputMessage();

        // When
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "alice", "bob" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "duct_taper", "alice" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "bob" );

        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContainsOnly( "alice", "bob" );
        assertRoleImportFileContains( "duct_taper", "alice" );
        assertRoleImportFileContains( "box_ticker","bob" );
        assertSuccessfulOutputMessage( 2 );
    }

    @Test
    public void shouldErrorGivenNonExistingUserFile() throws Throwable
    {
        // Given only a default role store file
        insertRole( ROLE_STORE_FILENAME, "taskmaster" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, "this_file_does_not_exist" );

        // Then
        verify( out ).stdErrLine( "command failed: " + getFile( "this_file_does_not_exist" ).getAbsolutePath() );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorGivenNonExistingRoleFile() throws Throwable
    {
        // Given only a default user store file
        insertUsers( USER_STORE_FILENAME, "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + ROLE_ARG_NAME, "this_file_does_not_exist" );

        // Then
        verify( out ).stdErrLine( "command failed: " + getFile( "this_file_does_not_exist" ).getAbsolutePath() );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorWithNonExistingDefaultUserAndRoleStoreFiles()
    {
        // Given no default user or role store files

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME );

        // Then
        verify( out ).stdErrLine( "command failed: " + getFile( USER_STORE_FILENAME ).getAbsolutePath() );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldOfflineImportAuthFromDefaultUserAndRoleStoreFiles() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode();
        assertSystemDbExists();
    }

    @Test
    public void shouldOfflineImportAuthIncrementally() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode();
        assertSystemDbExists();

        // When
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "mia" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "mia" );

        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode( 2 );
        assertSystemDbExists();
    }

    @Test
    public void shouldFailOfflineImportExistingUser() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode();
        assertSystemDbExists();

        // When
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "alice" ); // Already exists
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker" ); // Does not yet exist

        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        verify( out ).stdErrLine( "command failed: The specified user 'alice' already exists." );
        verify( out ).exit( 1 );
        assertSystemDbExists();
    }

    @Test
    public void shouldOfflineImportExistingUserAfterReset() throws Throwable
    {
        // Given
        insertUsers( USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + OFFLINE_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode();
        assertSystemDbExists();

        // When
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "alice" ); // Already exists
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker" ); // Does not yet exist

        tool.execute( homeDir.toPath(), confDir.toPath(), COMMAND_NAME,
                "--" + USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME,
                "--" + OFFLINE_ARG_NAME, "true",
                "--" + ImportAuthCommand.RESET_ARG_NAME, "true" );

        // Then
        assertSuccessfulOutputMessageForOfflineMode( 2 );
        assertSystemDbExists();
    }

    private void insertUsers( String filename, String... usernames ) throws Throwable
    {
        File userFile = getFile( filename );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, userFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        for ( String username : usernames )
        {
            userRepository.create( new User.Builder( username, LegacyCredential.INACCESSIBLE ).build() );
            assertTrue( userRepository.getAllUsernames().contains( username ) );
        }
        userRepository.stop();
        userRepository.shutdown();
    }

    private void insertRole( String filename, String roleName, String... usernames ) throws Throwable
    {
        File roleFile = getFile( filename );
        FileRoleRepository roleRepository = new FileRoleRepository( fileSystem, roleFile,
                NullLogProvider.getInstance() );
        roleRepository.start();
        RoleRecord.Builder builder = new RoleRecord.Builder().withName( roleName );
        for ( String username : usernames )
        {
            builder = builder.withUser( username );
        }
        roleRepository.create( builder.build() );
        assertTrue( roleRepository.getAllRoleNames().contains( roleName ) );
        for ( String username : usernames )
        {
            assertTrue( roleRepository.getRoleNamesByUsername( username ).contains( roleName ) );
        }
        roleRepository.stop();
        roleRepository.shutdown();
    }

    private void assertUserImportFileContainsOnly( String... usernames ) throws Throwable
    {
        File importFile = getFile( CommercialSecurityModule.USER_IMPORT_FILENAME );
        assertTrue( fileSystem.fileExists( importFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, importFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        Set<String> allUsernames = userRepository.getAllUsernames();
        assertThat( allUsernames, containsInAnyOrder( usernames ) );
    }

    private void assertRoleImportFileContains( String roleName, String... withOnlyUsernames ) throws Throwable
    {
        File importFile = getFile( CommercialSecurityModule.ROLE_IMPORT_FILENAME );
        assertTrue( fileSystem.fileExists( importFile ) );
        FileRoleRepository roleRepository = new FileRoleRepository( fileSystem, importFile,
                NullLogProvider.getInstance() );
        roleRepository.start();
        for ( String username : withOnlyUsernames )
        {
            assertThat( roleRepository.getRoleNamesByUsername( username ), containsInAnyOrder( roleName ) );
        }
    }

    private void assertNoUserImportFile()
    {
        assertFalse( fileSystem.fileExists( getFile( CommercialSecurityModule.USER_IMPORT_FILENAME ) ) );
    }

    private void assertNoRoleImportFile()
    {
        assertFalse( fileSystem.fileExists( getFile( CommercialSecurityModule.ROLE_IMPORT_FILENAME ) ) );
    }

    private void assertSuccessfulOutputMessage()
    {
        assertSuccessfulOutputMessage( 1 );
    }

    private void assertSuccessfulOutputMessage( int numberOfTimes )
    {
        verify( out, times( numberOfTimes ) ).stdOutLine( "Users and roles files copied to import files. " +
                "Please restart the database with configuration setting dbms.security.auth_provider=system-graph to complete the import." );
    }

    private void assertSuccessfulOutputMessageForOfflineMode()
    {
        assertSuccessfulOutputMessageForOfflineMode( 1 );
    }

    private void assertSuccessfulOutputMessageForOfflineMode( int numberOfTimes )
    {
        verify( out, times( numberOfTimes ) ).stdOutLine( "Users and roles files imported into system graph." );
    }

    private void assertSystemDbExists()
    {
        File systemDbStore = DatabaseLayout.of( getSystemDbDirectory() ).metadataStore();
        // NOTE: Because creating the system database is done on the real file system we cannot use our EphemeralFileSystemRule here
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        assertTrue( fs.fileExists( systemDbStore ) );
    }

    private File getSystemDbDirectory()
    {
        File databasesFolder = Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir.getAbsolutePath() ).get( GraphDatabaseSettings.databases_root_path );
        return DatabaseLayout.of( databasesFolder, IMPORT_SYSTEM_DATABASE_NAME ).databaseDirectory();
    }

    private File getFile( String name )
    {
        return new File( Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir.getAbsolutePath() )
                .get( DatabaseManagementSystemSettings.auth_store_directory ), name );
    }

    private void resetOutsideWorldMock()
    {
        reset( out );
        when( out.fileSystem() ).thenReturn( fileSystem );
    }

    private void resetSystemDatabaseDirectory() throws IOException
    {
        File systemDbDirectory = getSystemDbDirectory();
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        fs.deleteRecursively( systemDbDirectory );
    }
}
