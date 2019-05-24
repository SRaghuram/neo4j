/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SetDefaultAdminCommandTest
{
    private SetDefaultAdminCommand command;
    private File adminIniFile;
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private Config config;

    @Rule
    public ExpectedException expect = ExpectedException.none();
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory( fileSystem );

    @Before
    public void setup() throws IOException, InvalidArgumentsException
    {
        command = new SetDefaultAdminCommand( new ExecutionContext( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( PrintStream.class ), mock( PrintStream.class ), fileSystem ) );
        config = command.loadNeo4jConfig();
        UserRepository users = CommunitySecurityModule.getUserRepository( config, NullLogProvider.getInstance(),
                fileSystem );
        users.create(
                new User.Builder( "jake", LegacyCredential.forPassword( "123" ) )
                        .withRequiredPasswordChange( false )
                        .build()
            );
        adminIniFile = new File( CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile(), "admin.ini" );
    }

    @Test
    public void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo(
                "USAGE\n" +
                        "\n" +
                        "set-default-admin [--verbose] <username>\n" +
                        "\n" +
                        "DESCRIPTION\n" +
                        "\n" +
                        "Sets the default admin user when no roles are present.\n" +
                        "\n" +
                        "PARAMETERS\n" +
                        "\n" +
                        "      <username>\n" +
                        "\n" +
                        "OPTIONS\n" +
                        "\n" +
                        "      --verbose    Enable verbose output."
        ) );
    }

    @Test
    public void shouldSetDefaultAdmin() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( adminIniFile ) );

        // When
        CommandLine.populateCommand( command, "jake" );

        command.execute();

        // Then
        assertAdminIniFile( "jake" );
    }

    @Test
    public void shouldNotSetDefaultAdminForNonExistentUser() throws Throwable
    {
        // Then
        expect.expect( CommandFailedException.class );
        expect.expectMessage( "no such user: 'noName'" );

        // When
        CommandLine.populateCommand( command, "noName" );
        command.execute();
    }

    private void assertAdminIniFile( String username ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( adminIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, adminIniFile,
            NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.getAllUsernames(), containsInAnyOrder( username ) );
    }
}
