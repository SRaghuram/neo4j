/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@EphemeralTestDirectoryExtension
class SetOperatorPasswordCommandTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDir;

    private SetOperatorPasswordCommand command;
    private Path authOperatorFile;

    @BeforeEach
    void setup()
    {
        command = new SetOperatorPasswordCommand( new ExecutionContext( testDir.directory( "home" ),
                testDir.directory( "conf" ), mock( PrintStream.class ), mock( PrintStream.class ), fileSystem ) );

        authOperatorFile = EnterpriseSecurityModule.getOperatorUserRepositoryFile( command.loadNeo4jConfig() );
        CommunitySecurityModule.getUserRepositoryFile( command.loadNeo4jConfig() );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "USAGE%n" + "%n" +
                "set-operator-password [--verbose] <password>%n" +
                "%n" + "DESCRIPTION%n" + "%n" +
                "Sets the password of the operator user as defined by%n" +
                "'unsupported.dbms.upgrade_procedure_username'.%n" +
                "%n" + "PARAMETERS%n" + "%n" +
                "      <password>%n" + "%n" + "OPTIONS%n" + "%n" +
                "      --verbose    Enable verbose output." ) );
    }

    @Test
    void shouldSetOperatorPassword() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( authOperatorFile ) );

        // When
        CommandLine.populateCommand( command, "123" );
        command.execute();

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    void shouldOverwriteOperatorPasswordFileIfExists() throws Throwable
    {
        // Given
        fileSystem.mkdirs( authOperatorFile.getParent() );
        fileSystem.write( authOperatorFile );

        // When
        CommandLine.populateCommand( command, "321" );
        command.execute();

        // Then
        assertAuthIniFile( "321" );
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( authOperatorFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authOperatorFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        User operator = userRepository.getUserByName( Config.defaults().get( GraphDatabaseInternalSettings.upgrade_username ) );
        assertNotNull( operator );
        assertTrue( operator.credentials().matchesPassword( password.getBytes( StandardCharsets.UTF_8 ) ) );
        assertFalse( operator.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }
}
