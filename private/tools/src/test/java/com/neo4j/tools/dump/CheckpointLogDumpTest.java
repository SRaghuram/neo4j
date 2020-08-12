/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import static com.neo4j.tools.dump.CheckpointLogDump.dumpCheckpoints;
import static org.assertj.core.api.Assertions.assertThat;

class CheckpointLogDumpTest
{
    private PrintStream printStream;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp()
    {
        outputStream = new ByteArrayOutputStream();
        printStream = new PrintStream( outputStream );
    }

    @Test
    void dumpSingleFileContent() throws IOException
    {
        var resourceFile = getResourceFile();
        dumpCheckpoints( resourceFile, printStream );
        assertThat( outputStream.toString() ).containsOnlyOnce( "LogEntryDetachedCheckpoint" )
                                             .containsOnlyOnce( "Checkpoint triggered by \"Database shutdown\" @ txId: 1" );
    }

    @Test
    void dumpDirectoryContent() throws IOException
    {
        var resourceFile = getResourceFile().getParentFile();
        dumpCheckpoints( resourceFile, printStream );
        assertThat( outputStream.toString() ).satisfies( s -> assertThat( StringUtils.countMatches( s, "LogEntryDetachedCheckpoint" ) ).isEqualTo( 2 ) )
                                             .containsOnlyOnce( "reason='Checkpoint triggered by \"Database shutdown\" @ txId: 1'" )
                                            .containsOnlyOnce( "reason='test'" );
    }

    private File getResourceFile()
    {
        return new File( getClass().getResource( "/checkpoint.0" ).getFile() );
    }
}