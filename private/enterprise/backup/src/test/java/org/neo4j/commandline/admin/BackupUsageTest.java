/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.commandline.admin;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.backup.impl.BackupHelpOutput;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@ExtendWith( SuppressOutputExtension.class )
class BackupUsageTest
{
    private static final Path HERE = Paths.get( "." );

    @Inject
    private SuppressOutput suppressOutput;

    private final CommandLocator commandLocator = CommandLocator.fromServiceLocator();

    @Test
    void outputMatchesExpectedForMissingBackupDir()
    {
        // when
        String output = runBackup();

        // then
        String reason = "Missing argument 'backup-dir'";
        assertThat( output, containsString( reason ) );

        // and
        for ( String line : BackupHelpOutput.BACKUP_OUTPUT_LINES )
        {
            assertThat( output, containsString( line ) );
        }
    }

    @Test
    void incorrectBackupDirectory() throws IOException
    {
        // when
        Path backupDirectoryResolved = HERE.toRealPath().resolve( "non_existing_dir" );
        String output = runBackup( "--backup-dir=non_existing_dir" );

        // then
        String reason = String.format( "command failed: Directory '%s' does not exist.", backupDirectoryResolved );
        assertThat( output, containsString( reason ) );
    }

    private String runBackup( String... args )
    {
        return runBackup( false, args );
    }

    private String runBackup( boolean debug, String... args )
    {
        ParameterisedOutsideWorld outsideWorld = // ParameterisedOutsideWorld used for suppressing #close() doing System.exit()
                new ParameterisedOutsideWorld( System.console(), System.out, System.err, System.in, new DefaultFileSystemAbstraction() );
        AdminTool subject = new AdminTool( commandLocator, outsideWorld, debug );
        List<String> params = new ArrayList<>();
        params.add( "backup" );
        params.addAll( Arrays.asList( args ) );
        String[] argArray = params.toArray( new String[0] );
        subject.execute( HERE, HERE, argArray );

        return suppressOutput.getErrorVoice().toString() + System.lineSeparator() + suppressOutput.getOutputVoice().toString();
    }
}
