/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.process.ProcessWrapper;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.neo4j.bench.common.util.BenchmarkUtil.forceRecreateFile;
import static com.neo4j.bench.common.util.BenchmarkUtil.inputStreamToString;
import static java.lang.ProcessBuilder.Redirect;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

class Neo4jServerWrapper
{
    // NOTE: specifying neo4j configuration file location via env var should work, but neo4j does not seem to pick up the config
    private static final boolean CONFIG_VIA_ENV_VAR_WORKS = false;

    private static final String EXE_PATH = "bin/neo4j";
    private static final String NEO4J_LOG_PATH = "logs/neo4j.log";
    private static final String DEBUG_LOG_PATH = "logs/debug.log";
    private static final String NEO4J_CONF_PATH = "conf/neo4j.conf";
    private static final String NEO4J_CONFIG_ENV_VAR = "NEO4J_CONF";
    private static final String NEO4J_IS_RUNNING_STATUS_PREFIX = "Neo4j is running at pid";

    private final Path neo4jDir;

    Neo4jServerWrapper( Path neo4jDir )
    {
        this.neo4jDir = neo4jDir;
    }

    void clearLogs()
    {
        forceRecreateFile( neo4jDir.resolve( NEO4J_LOG_PATH ) );
        forceRecreateFile( neo4jDir.resolve( DEBUG_LOG_PATH ) );
    }

    void copyLogsTo( Path destinationFolder )
    {
        try
        {
            BenchmarkUtil.assertDirectoryExists( destinationFolder );
            Files.copy( neo4jDir.resolve( NEO4J_LOG_PATH ),
                        destinationFolder.resolve( neo4jDir.resolve( NEO4J_LOG_PATH ).getFileName() ) );
            Files.copy( neo4jDir.resolve( DEBUG_LOG_PATH ),
                        destinationFolder.resolve( neo4jDir.resolve( DEBUG_LOG_PATH ).getFileName() ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error while trying to copy Neo4j logs\n" +
                                            "From : " + neo4jDir.toAbsolutePath() + "\n" +
                                            "To   : " + destinationFolder.toAbsolutePath(), e );
        }
    }

    Neo4jServerConnection start( Jvm jvm, Path neo4jConfigFile, Redirect outputRedirect, Redirect errorRedirect )
    {
        if ( isRunning() )
        {
            throw new RuntimeException( "Neo4j server is already running" );
        }
        try
        {
            ProcessBuilder processBuilder = neo4jCommand( outputRedirect, errorRedirect, "start" );

            if ( neo4jConfigFile != null && CONFIG_VIA_ENV_VAR_WORKS )
            {
                // Specify location of configuration file using environment variable
                processBuilder.environment().put( NEO4J_CONFIG_ENV_VAR, neo4jConfigFile.toAbsolutePath().toString() );
            }
            else
            {
                // Overwrite neo4j.conf in neo4j server directory
                // Should not be necessary, but neo4j does not seem to pick up the configuration when its location is specified using environment variable
                writeConfig( neo4jConfigFile );
            }
            Path jdkPath = jvm.jdkPath().orElseThrow( () -> new RuntimeException( "Can not launch Neo4j Server without knowing JDK path" ) );
            processBuilder.environment().put( "JAVA_HOME", jdkPath.toAbsolutePath().toString() );
            processBuilder.start();
            List<String> neo4jLogLines = waitForServerAndGetOutput( outputRedirect );
            URI boltUri = extractBoltUriOrFail( neo4jLogLines );
            return new Neo4jServerConnection( boltUri, pid() );
        }
        catch ( Exception e )
        {
            try
            {
                stop();
            }
            catch ( TimeoutException timeoutException )
            {
                // print stack trace but do not rethrow, as this is the not the original cause of failure
                timeoutException.printStackTrace();
            }
            throw new RuntimeException( "Was unable to start Neo4j server\n" +
                                        "-------------------- Debug Log --------------------\n" +
                                        debugLogString(), e );
        }
    }

    private String debugLogString()
    {
        try
        {
            return String.join( "\n", Files.readAllLines( neo4jDir.resolve( DEBUG_LOG_PATH ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error trying to read debug log", e );
        }
    }

    private void writeConfig( Path sourceNeo4jConfigFile )
    {
        Path destinationNeo4jConfigFile = neo4jDir.resolve( NEO4J_CONF_PATH );
        try
        {
            Files.copy( sourceNeo4jConfigFile, destinationNeo4jConfigFile, StandardCopyOption.REPLACE_EXISTING );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error while trying to copy Neo4j configuration file\n" +
                                            "From : " + sourceNeo4jConfigFile.toAbsolutePath() + "\n" +
                                            "To   : " + destinationNeo4jConfigFile.toAbsolutePath(), e );
        }
    }

    boolean isRunning()
    {
        return status().startsWith( NEO4J_IS_RUNNING_STATUS_PREFIX );
    }

    // Returns server PID if the server is running, and nothing otherwise
    private Pid pid()
    {
        String neo4jServerStatus = status();
        if ( !neo4jServerStatus.contains( NEO4J_IS_RUNNING_STATUS_PREFIX ) )
        {
            throw new RuntimeException( "Neo4j server is not running\n" +
                                        "Status: " + neo4jServerStatus );
        }
        int pidOffset = neo4jServerStatus.indexOf( NEO4J_IS_RUNNING_STATUS_PREFIX ) + NEO4J_IS_RUNNING_STATUS_PREFIX.length();
        return new Pid( Long.parseLong( neo4jServerStatus.substring( pidOffset ).trim() ) );
    }

    void stop() throws TimeoutException
    {
        ProcessWrapper.start( neo4jCommand( INHERIT, INHERIT, "stop" ) ).waitFor();
        EventualValue<String> eventualValue = new EventualValue<>()
        {
            @Override
            public String getValueOrNull()
            {
                return isRunning() ? null : "Neo4j server successfully stopped";
            }

            @Override
            public String getTimedOutMessage()
            {
                return "Status: " + status();
            }
        };
        waitFor( eventualValue );
    }

    private String status()
    {
        try
        {
            Process statusProcess = neo4jCommand( null, null, "status" ).start();
            int resultCode = statusProcess.waitFor();
            String statusOutput = inputStreamToString( statusProcess.getInputStream() );
            // result code seems to be three when the database is not running
            if ( !asList( 0, 3 ).contains( resultCode ) )
            {
                throw new RuntimeException( "Bad things happened while trying to retrieve pid of Neo4j server\n" +
                                            "Code   : " + resultCode + "\n" +
                                            "Output : " + statusOutput );
            }
            return statusOutput;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving Neo4j server status", e );
        }
    }

    private ProcessBuilder neo4jCommand( Redirect outputRedirect,
                                         Redirect errorRedirect,
                                         String command )
    {
        Path neo4jExe = neo4jDir.resolve( EXE_PATH );
        ProcessBuilder processBuilder = new ProcessBuilder()
                .command( "bash",
                          neo4jExe.toAbsolutePath().toString(),
                          command );
        if ( outputRedirect != null )
        {
            processBuilder.redirectOutput( outputRedirect );
        }
        if ( errorRedirect != null )
        {
            processBuilder.redirectError( errorRedirect );
        }
        return processBuilder;
    }

    /**
     * Waits for Neo4j Server to start.
     * Check for started is done by intermittently reading in the process output and neo4j.log.
     *
     * @param output a redirect of neo4j server process output
     * @return neo4j server output, as list of lines from both process output and neo4j.log -- process output lines precede neo4j.log lines.
     */
    private List<String> waitForServerAndGetOutput( Redirect output ) throws TimeoutException
    {
        EventualValue<List<String>> eventualValue = new EventualValue<>()
        {
            @Override
            public List<String> getValueOrNull()
            {
                List<String> outputLines = getOutputLines();
                boolean logContainsStartedEntry = outputLines.stream().anyMatch( line -> line.endsWith( "Started." ) );
                return logContainsStartedEntry ? outputLines : null;
            }

            @Override
            public String getTimedOutMessage()
            {
                return String.join( "\n", getOutputLines() );
            }

            private List<String> getOutputLines()
            {
                List<String> outputLines = new ArrayList<>();
                try
                {
                    List<String> processOutputLines = output.file() == null
                                                      ? emptyList()
                                                      : Files.readAllLines( output.file().toPath() );
                    List<String> neo4jLogLines = Files.readAllLines( neo4jDir.resolve( NEO4J_LOG_PATH ) );
                    outputLines.add( "*********** Process Output ***********" );
                    outputLines.addAll( processOutputLines );
                    outputLines.add( "********** Neo4j Log Output **********" );
                    outputLines.addAll( neo4jLogLines );
                    outputLines.add( "**************************************" );
                    return outputLines;
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( "Complete Process Output:\n" +
                                                    String.join( "\n", outputLines ),
                                                    e );
                }
            }
        };

        return waitFor( eventualValue );
    }

    private interface EventualValue<T>
    {
        T getValueOrNull();

        String getTimedOutMessage();
    }

    private <T> T waitFor( EventualValue<T> eventualValue ) throws TimeoutException
    {
        try
        {
            Instant start = now();
            Duration timeoutDuration = Duration.ofMinutes( 5 );

            while ( !hasTimedOut( start, timeoutDuration ) )
            {
                T value = eventualValue.getValueOrNull();
                if ( value != null )
                {
                    return value;
                }
                Thread.sleep( 1000 );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Something went wrong while waiting", e );
        }
        throw new TimeoutException( "Grew tired of waiting!\n" +
                                    "===============================================\n" +
                                    eventualValue.getTimedOutMessage() );
    }

    private boolean hasTimedOut( Instant start, Duration timeoutDuration )
    {
        Duration durationWaited = between( start, now() );
        return durationWaited.compareTo( timeoutDuration ) >= 0;
    }

    /**
     * Parses neo4j.log file to discover bolt end-point URI of neo4j server
     *
     * @param neo4jLogLines neo4j.log log lines
     * @return bolt URI
     */
    private URI extractBoltUriOrFail( List<String> neo4jLogLines )
    {
        String boltEnabledPrefix = "Bolt enabled on ";
        List<String> boltEnabledLines = neo4jLogLines.stream().filter( line -> line.contains( boltEnabledPrefix ) ).collect( toList() );
        if ( boltEnabledLines.isEmpty() )
        {
            throw new RuntimeException( "Unable to find Bolt URI in log output.\n\n" +
                                        String.join( "\n", neo4jLogLines ) );
        }
        else if ( boltEnabledLines.size() > 1 )
        {
            throw new RuntimeException( format( "Expected to find 0-1 lines starting with '%s', but found: %s.\n\n%s",
                                                String.join( "\n", neo4jLogLines ),
                                                boltEnabledPrefix,
                                                boltEnabledLines.size() ) );
        }
        else
        {
            String boltEnabledLine = boltEnabledLines.get( boltEnabledLines.size() - 1 );
            int startIndex = boltEnabledLine.indexOf( boltEnabledPrefix ) + boltEnabledPrefix.length();
            // Need -1 because line ends in '.'
            // E.g., '2019-03-23 12:07:23.188+0000 INFO  Bolt enabled on 127.0.0.1:7687.'
            int endIndex = boltEnabledLine.length() - 1;
            String boltUriString = "bolt://" + boltEnabledLine.substring( startIndex, endIndex ).trim();
            return URI.create( boltUriString );
        }
    }

    static class Neo4jServerConnection
    {
        private final URI boltUri;
        private final Pid pid;

        private Neo4jServerConnection( URI boltUri, Pid pid )
        {
            this.boltUri = boltUri;
            this.pid = pid;
        }

        URI boltUri()
        {
            return boltUri;
        }

        Pid pid()
        {
            return pid;
        }
    }
}
