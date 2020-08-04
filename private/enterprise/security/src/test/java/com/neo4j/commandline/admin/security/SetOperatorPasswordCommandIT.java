/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith( EphemeralFileSystemExtension.class )
class SetOperatorPasswordCommandIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    private File confDir;
    private File homeDir;
    private PrintStream out;
    private PrintStream err;

    private String upgradeUsername = Config.defaults().get( GraphDatabaseInternalSettings.upgrade_username );

    @BeforeEach
    void setup()
    {
        File graphDir = new File( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
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

        verify( out ).println( String.format( "Changed password for operator user '%s'.", upgradeUsername ) );
    }

    @Test
    void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );
        executeCommand( "muchBetter" );
        assertAuthIniFile( "muchBetter" );

        verify( out, times( 2 ) ).println( String.format( "Changed password for operator user '%s'.", upgradeUsername ) );
    }

    @Test
    void shouldWorkWithSamePassword() throws Throwable
    {
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );

        verify( out, times( 2 ) ).println( String.format( "Changed password for operator user '%s'.", upgradeUsername ) );
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        File authIniFile = getOperatorAuthFile();
        assertTrue( fileSystem.fileExists( authIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authIniFile, NullLogProvider.getInstance() );
        userRepository.start();
        User operator = userRepository.getUserByName( upgradeUsername );
        assertNotNull( operator );
        assertTrue( operator.credentials().matchesPassword( password.getBytes( StandardCharsets.UTF_8 ) ) );
        assertThat( operator.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) ).isEqualTo( false );
    }

    private File getOperatorAuthFile()
    {
        return new File( new File( new File( homeDir, "data" ), "dbms" ), EnterpriseSecurityModule.OPERATOR_STORE_FILENAME );
    }

    private void executeCommand( String... args )
    {
        final var ctx = new ExecutionContext( homeDir.toPath(), confDir.toPath(), out, err, fileSystem );
        final var command = new SetOperatorPasswordCommand( ctx );
        CommandLine.populateCommand( command, args );
        command.execute();
    }
}
