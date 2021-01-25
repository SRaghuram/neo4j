/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class ProcessFailureException extends Exception
{
    private static final String INDENT = "          ";

    public static String getProcessOutputAsString( Process process )
    {
        try ( BufferedReader buffer = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) )
        {
            return linesToString( buffer.lines() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error while trying to read process output", e );
        }
    }

    private static String outputLogToString( Path processOutputLog )
    {
        try ( Stream<String> lines = Files.lines( processOutputLog ) )
        {
            return linesToString( lines );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error while trying to read output log", e );
        }
    }

    private static String linesToString( Stream<String> lines )
    {
        return lines.map( line -> INDENT + line ).collect( joining( "\n" ) );
    }

    private static String processOutputErrorString( String output )
    {
        return format( "Failed process output:%n" +
                       "================================================================================================%n" +
                       "================================================================================================%n" +
                       "%s%n" +
                       "================================================================================================%n" +
                       "================================================================================================",
                       output );
    }

    public ProcessFailureException( String message, Path processOutputLog, Throwable cause )
    {
        super( format( "%s%n%s", message, processOutputErrorString( outputLogToString( processOutputLog ) ) ), cause );
    }

    public ProcessFailureException( String message, Process failedProcess, Throwable cause )
    {
        super( format( "%s%n%s", message, processOutputErrorString( getProcessOutputAsString( failedProcess ) ) ), cause );
    }

    public ProcessFailureException( String message, Path processOutputLog )
    {
        super( format( "%s%n%s", message, processOutputErrorString( outputLogToString( processOutputLog ) ) ) );
    }

    public ProcessFailureException( String message, Process failedProcess )
    {
        super( format( "%s%n%s", message, processOutputErrorString( getProcessOutputAsString( failedProcess ) ) ) );
    }
}
