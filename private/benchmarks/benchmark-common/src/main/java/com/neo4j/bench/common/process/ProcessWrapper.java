/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import static com.neo4j.bench.common.util.BenchmarkUtil.inputStreamToString;

public class ProcessWrapper implements BaseProcess
{
    // TODO in JMH the parent & forked processes communicate via ports, that may be interesting for future work

    // TODO consider stealing "stream drainer" concept from JMH too

    public static ProcessWrapper start( ProcessBuilder processBuilder )
    {
        List<String> args = processBuilder.command();
        File outputFile = processBuilder.redirectOutput().file();
        File errorFile = processBuilder.redirectError().file();
        try
        {
            Process process = processBuilder.start();

            return new ProcessWrapper(
                    args,
                    process,
                    (null != outputFile) ? outputFile.toPath() : null,
                    (null != errorFile) ? errorFile.toPath() : null );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error starting process\n" +
                                        "Args: " + args, e );
        }
    }

    private final List<String> args;
    private final Process process;
    private final Path output;
    private final Path error;

    private ProcessWrapper( List<String> args, Process process, Path output, Path error )
    {
        this.args = args;
        this.process = process;
        this.output = output;
        this.error = error;
    }

    String infoString()
    {
        String baseInfo = "====================================================================\n" +
                          "--- Args ---\n" +
                          "--------------------------------------------------------------------\n" +
                          args + "\n" +
                          "====================================================================\n" +
                          "--- Process Output ---\n" +
                          "--------------------------------------------------------------------\n" +
                          output( output, process, Process::getInputStream ) + "\n";

        return isOutputSameAsError()

               ? baseInfo +
                 "====================================================================\n"

               : baseInfo +
                 "====================================================================\n" +
                 "--- Process Error ---\n" +
                 "--------------------------------------------------------------------\n" +
                 output( error, process, Process::getErrorStream ) + "\n" +
                 "====================================================================\n";
    }

    private boolean isOutputSameAsError()
    {
        return (null == output && null == error) ||
               (null != output && output.equals( error ));
    }

    private static String output( Path output, Process process, Function<Process,InputStream> processInputStream )
    {
        try
        {
            // if no file exists include that info in printout, to assist with debugging -- it may be surprising
            String outputFileInfo = (null != output && !Files.exists( output ))
                                    ? "Expected to find output file, but none found.\n" +
                                      "Path: " + output.toAbsolutePath().toString() + "\n\n"
                                    : "";
            // expect to find an output file & find one
            if ( null != output && Files.exists( output ) )
            {
                return outputFileInfo + inputStreamToString( Files.newInputStream( output ) );
            }
            else if ( process.isAlive() )
            {
                return outputFileInfo + inputStreamToString( processInputStream.apply( process ) );
            }
            else
            {
                return outputFileInfo + "<UNABLE TO RETRIEVE PROCESS OUTPUT>";
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error retrieving process output", e );
        }
    }

    @Override
    public void waitFor()
    {
        try
        {
            int code = process.waitFor();
            if ( 0 != code )
            {
                throw new RuntimeException( "Fork exited with code: " + code + "\n" +
                                            infoString() );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Error while waiting for process", e );
        }
    }

    @Override
    public void stop()
    {
        if ( null == process )
        {
            return;
        }
        try
        {
            int code = process.destroyForcibly().waitFor();
            if ( process.isAlive() )
            {
                throw new RuntimeException( "Failed to stop process. Code: " + code + "\n" +
                                            infoString() );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Interrupted while trying to stop process\n" +
                                        infoString(),
                                        e );
        }
    }
}
