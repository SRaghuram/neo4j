/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.admin.commands;

import com.neo4j.backup.impl.OnlineBackupCommand;
import com.neo4j.commandline.admin.security.SetOperatorPasswordCommand;
import com.neo4j.commandline.dbms.EnterpriseLoadCommand;
import com.neo4j.commandline.dbms.UnbindFromClusterCommand;
import com.neo4j.dbms.commandline.StoreCopyCommand;
import com.neo4j.restore.RestoreDatabaseCli;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@TestDirectoryExtension
// Config files created in our build server doesn't get the right permissions,
// However this test is only interested in the parsing of config entries with --expand-command option, so not necessary to run it on Windows.
@DisabledOnOs( OS.WINDOWS )
public class AdminCommandsIT
{
    @Inject
    private TestDirectory testDirectory;
    private Path confDir;
    private PrintStream out;
    private PrintStream err;
    private ExecutionContext context;

    @BeforeEach
    void setup() throws Exception
    {
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
        confDir = testDirectory.directory( "test.conf" );
        context = new ExecutionContext( testDirectory.homePath(), confDir, out, err, testDirectory.getFileSystem() );
        Path configFile = confDir.resolve( "neo4j.conf" );
        Files.createFile( configFile, PosixFilePermissions.asFileAttribute( Set.of( OWNER_READ, OWNER_WRITE ) ) );
        GraphDatabaseSettings.strict_config_validation.name();
        Files.write( configFile, (BootloaderSettings.initial_heap_size.name() + "=$(expr 500)").getBytes() );
    }

    @Test
    void shouldExpandCommands() throws Exception
    {
        assertSuccess( new SetOperatorPasswordCommand( context ), "--expand-commands", "pass" );
        assertSuccess( new StoreCopyCommand( context ), "--expand-commands", "--from-database", "foo", "--to-database", "bar" );
        assertSuccess( new OnlineBackupCommand( context ), "--expand-commands", "--backup-dir",
                       testDirectory.directory( "backup" ).toAbsolutePath().toString() );
        assertSuccess( new RestoreDatabaseCli( context ), "--expand-commands", "--from", "foo" );
        assertSuccess( new EnterpriseLoadCommand( context, new Loader() ), "--expand-commands", "--from", "foo" );
        assertSuccess( new UnbindFromClusterCommand( context ), "--expand-commands" );
    }

    @Test
    void shouldNotExpandCommands()
    {
        assertExpansionError( new SetOperatorPasswordCommand( context ), "pass" );
        assertExpansionError( new StoreCopyCommand( context ), "--from-database", "foo", "--to-database", "bar" );
        assertExpansionError( new OnlineBackupCommand( context ), "--backup-dir", testDirectory.directory( "backup" ).toAbsolutePath().toString() );
        assertExpansionError( new RestoreDatabaseCli( context ), "--from", "foo" );
        assertExpansionError( new EnterpriseLoadCommand( context, new Loader() ), "--from", "foo" );
        assertExpansionError( new UnbindFromClusterCommand( context ) );
    }

    private void assertSuccess( AbstractCommand command, String... args ) throws Exception
    {
        CommandLine.populateCommand( command, args ).call();
    }

    private void assertExpansionError( AbstractCommand command, String... args )
    {
        var exception = new MutableObject<Exception>();
        new CommandLine( command ).setExecutionExceptionHandler(
                ( ex, commandLine, result ) ->
                {
                    exception.setValue( ex );
                    return 1;
                } ).execute( args );
        assertThat( exception.getValue() ).hasMessageContaining( "is a command, but config is not explicitly told to expand it." );
    }
}
