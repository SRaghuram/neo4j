/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.BlockerLocator;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.enterprise.auth.EnterpriseSecurityModule;
import org.neo4j.server.security.enterprise.auth.FileRoleRepository;
import org.neo4j.server.security.enterprise.auth.RoleRecord;

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

public class ImportAuthCommandIT
{
    private static final String ALTERNATIVE_USER_STORE_FILENAME = "users_to_import";
    private static final String ALTERNATIVE_ROLE_STORE_FILENAME = "roles_to_import";

    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private File confDir;
    private File homeDir;
    private OutsideWorld out;
    private AdminTool tool;

    @Before
    public void setup()
    {
        File graphDir = new File( DatabaseManager.DEFAULT_DATABASE_NAME );
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
        out = mock( OutsideWorld.class );
        resetOutsideWorldMock();
        tool = new AdminTool( CommandLocator.fromServiceLocator(), BlockerLocator.fromServiceLocator(), out, true );
    }

    @Test
    public void shouldImportAuthFromDefaultUserAndRoleStoreFiles() throws Throwable
    {
        // Given
        insertUsers( CommunitySecurityModule.USER_STORE_FILENAME, "jane", "alice", "bob", "jim" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME,  "flunky", "jane" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME );

        // Then
        assertUserImportFileContains( "jane", "alice", "bob", "jim" );
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
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "flunky", "jane" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "goon", "bob", "jim" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "taskmaster", "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME );

        // Then
        assertUserImportFileContains( "jane", "alice", "bob", "jim" );
        assertRoleImportFileContains( "flunky", "jane" );
        assertRoleImportFileContains( "goon", "bob", "jim" );
        assertRoleImportFileContains( "taskmaster", "alice" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldImportAuthWithGivenRoleStoreFile() throws Throwable
    {
        // Given
        insertUsers( CommunitySecurityModule.USER_STORE_FILENAME, "alice", "bob" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "duct_taper", "alice" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "bob" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContains( "alice", "bob" );
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
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ImportAuthCommand.ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContains( "alice", "bob" );
        assertRoleImportFileContains( "duct_taper", "alice" );
        assertRoleImportFileContains( "box_ticker","bob" );
        assertSuccessfulOutputMessage();
    }

    @Test
    public void shouldOverwriteImportFiles() throws Throwable
    {
        // Given
        insertUsers( CommunitySecurityModule.USER_STORE_FILENAME, "jim", "john" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME,  "flunky", "jim" );
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "goon", "john" );

        assertNoUserImportFile();
        assertNoRoleImportFile();

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME );

        // Then
        assertUserImportFileContains( "jim", "john" );
        assertRoleImportFileContains( "flunky","jim" );
        assertRoleImportFileContains( "goon","john" );
        assertSuccessfulOutputMessage();

        // When
        insertUsers( ALTERNATIVE_USER_STORE_FILENAME, "alice", "bob" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "duct_taper", "alice" );
        insertRole( ALTERNATIVE_ROLE_STORE_FILENAME, "box_ticker", "bob" );

        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.USER_ARG_NAME, ALTERNATIVE_USER_STORE_FILENAME,
                "--" + ImportAuthCommand.ROLE_ARG_NAME, ALTERNATIVE_ROLE_STORE_FILENAME );

        // Then
        assertUserImportFileContains( "alice", "bob" );
        assertRoleImportFileContains( "duct_taper", "alice" );
        assertRoleImportFileContains( "box_ticker","bob" );
        assertSuccessfulOutputMessage( 2 );
    }

    @Test
    public void shouldErrorGivenNonExistingUserFile() throws Throwable
    {
        // Given only a default role store file
        insertRole( EnterpriseSecurityModule.ROLE_STORE_FILENAME, "taskmaster" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.USER_ARG_NAME, "this_file_does_not_exist" );

        // Then
        verify( out ).stdErrLine( "command failed: File " + getFile( "this_file_does_not_exist" ).getAbsolutePath() + " not found" );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorGivenNonExistingRoleFile() throws Throwable
    {
        // Given only a default user store file
        insertUsers( CommunitySecurityModule.USER_STORE_FILENAME, "alice" );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME,
                "--" + ImportAuthCommand.ROLE_ARG_NAME, "this_file_does_not_exist" );

        // Then
        verify( out ).stdErrLine( "command failed: File " + getFile( "this_file_does_not_exist" ).getAbsolutePath() + " not found" );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorWithNonExistingDefaultUserAndRoleStoreFiles()
    {
        // Given no default user or role store files

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), ImportAuthCommand.COMMAND_NAME );

        // Then
        verify( out ).stdErrLine( "command failed: File " + getFile( CommunitySecurityModule.USER_STORE_FILENAME ).getAbsolutePath() + " not found" );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
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

    private void assertUserImportFileContains( String... usernames ) throws Throwable
    {
        File importFile = getFile( ImportAuthCommand.USER_IMPORT_FILENAME );
        assertTrue( fileSystem.fileExists( importFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, importFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        Set<String> allUsernames = userRepository.getAllUsernames();
        assertThat( allUsernames, containsInAnyOrder( usernames ) );
    }

    private void assertRoleImportFileContains( String roleName, String... usernames ) throws Throwable
    {
        File importFile = getFile( ImportAuthCommand.ROLE_IMPORT_FILENAME );
        assertTrue( fileSystem.fileExists( importFile ) );
        FileRoleRepository roleRepository = new FileRoleRepository( fileSystem, importFile,
                NullLogProvider.getInstance() );
        roleRepository.start();
        for ( String username : usernames )
        {
            assertThat( roleRepository.getRoleNamesByUsername( username ), containsInAnyOrder( roleName ) );
        }
    }

    private void assertNoUserImportFile()
    {
        assertFalse( fileSystem.fileExists( getFile( ImportAuthCommand.USER_IMPORT_FILENAME ) ) );
    }

    private void assertNoRoleImportFile()
    {
        assertFalse( fileSystem.fileExists( getFile( ImportAuthCommand.ROLE_IMPORT_FILENAME ) ) );
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

    private File getFile( String name )
    {
        return new File( new File( new File( homeDir, "data" ), "dbms" ), name );
    }

    private void resetOutsideWorldMock()
    {
        reset( out );
        when( out.fileSystem() ).thenReturn( fileSystem );
    }
}
