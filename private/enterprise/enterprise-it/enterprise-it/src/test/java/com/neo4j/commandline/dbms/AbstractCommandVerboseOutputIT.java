/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.commandline.dbms.AbstractCommandIT.Output;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractCommandVerboseOutputIT
{
    @Test
    void printVerboseInfo() throws Exception
    {
        Output output = new Output();
        var context = new ExecutionContext( Path.of( "." ), Path.of( "." ), output.printStream, output.printStream, new DefaultFileSystemAbstraction() );
        var command = new DummyCommand( context );

        String[] args = {"--verbose"};
        CommandLine.populateCommand( command, args );

        command.call();
        assertTrue( output.containsMessage( "VM Vendor" ) );
    }

    private static class DummyCommand extends AbstractCommand
    {
        protected DummyCommand( ExecutionContext ctx )
        {
            super( ctx );
        }

        @Override
        protected void execute()
        {

        }
    }
}
