/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.nio.file.Path;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( SuppressOutputExtension.class )
class AbstractCommandVerboseOutputIT
{
    @Inject
    SuppressOutput suppressOutput;

    @Test
    void printVerboseInfo() throws Exception
    {
        var context = new ExecutionContext( Path.of( "." ), Path.of( "." ) );
        var command = new DummyCommand( context );

        String[] args = {"--verbose"};
        CommandLine.populateCommand( command, args );

        command.call();
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "VM Vendor" ) );
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
