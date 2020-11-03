/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith( EphemeralFileSystemExtension.class )
class SetOperatorPasswordCommandIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;

    @BeforeEach
    void setup()
    {
        Path graphDir = Path.of( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        confDir = graphDir.resolve( "conf" );
        homeDir = graphDir.resolve( "home" );
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fileSystem.close();
    }

    @Test
    void shouldSetPassword() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );

        verify( out ).println( "Changed password for operator user." );
    }

    @Test
    void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );
        executeCommand( "muchBetter" );
        assertAuthIniFile( "muchBetter" );

        verify( out, times( 2 ) ).println( "Changed password for operator user." );
    }

    @Test
    void shouldWorkWithSamePassword() throws Throwable
    {
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );

        verify( out, times( 2 ) ).println( "Changed password for operator user." );
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        Path authIniFile = getOperatorAuthFile();
        assertThat( fileSystem.fileExists( authIniFile ) ).isTrue();
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authIniFile, NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.numberOfUsers() ).isEqualTo( 1 );
        Optional<User> operator = userRepository.getAllUsernames().stream().map( userRepository::getUserByName ).findFirst();
        assertThat( operator.isPresent() ).isTrue();
        assertThat( operator.get().credentials().matchesPassword( password.getBytes( StandardCharsets.UTF_8 ) ) ).isTrue();
        assertThat( operator.get().hasFlag( User.PASSWORD_CHANGE_REQUIRED ) ).isFalse();
    }

    private Path getOperatorAuthFile()
    {
        return homeDir.resolve( "data" ).resolve( "dbms" ).resolve( EnterpriseSecurityModule.OPERATOR_STORE_FILENAME );
    }

    private void executeCommand( String... args )
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, args );
        command.execute();
    }
}
