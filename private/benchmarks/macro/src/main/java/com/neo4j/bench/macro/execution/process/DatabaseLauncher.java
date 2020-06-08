/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunSingleEmbeddedCommand;
import com.neo4j.bench.macro.cli.RunSingleServerCommand;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.workload.Query;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.connectors.BoltConnector;

import static java.lang.ProcessBuilder.Redirect;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

public abstract class DatabaseLauncher<CONNECTION extends AutoCloseable>
{
    /**
     * Only use profilers that are both not-external.
     * Some profilers implement both internal & external interfaces, those are not safe to use when the client is not in a separate fork.
     *
     * @param isProcessForked specifies if the client (benchmark executing) process is a fork or not.
     * @return filtered internal profilers -- those that are safe to run, given the fork status of the process.
     */
    static List<ProfilerType> internalProfilers( List<ProfilerType> profilerTypes, boolean isProcessForked )
    {
        if ( isProcessForked )
        {
            return profilerTypes;
        }
        else
        {
            List<ProfilerType> nonExternalProfilerTypes = new ArrayList<>( profilerTypes );
            List<ProfilerType> externalProfilers = ProfilerType.externalProfilers( profilerTypes );
            nonExternalProfilerTypes.removeAll( externalProfilers );
            if ( !externalProfilers.isEmpty() )
            {
                String warningMessage = "\n" +
                                        "-------------------------------------------------------------------------------------------------\n" +
                                        "-------------------------------------------  WARNING  -------------------------------------------\n" +
                                        "-------------------------------------------------------------------------------------------------\n" +
                                        "Profilers that depend on 'JVM Args' and/or 'Process Invoke Args' might not work in non-forking mode\n" +
                                        "The follow profilers will not be used on the client (load generating) process: " + externalProfilers + "\n" +
                                        "-------------------------------------------------------------------------------------------------\n";
                System.out.println( warningMessage );
            }
            return nonExternalProfilerTypes;
        }
    }

    final int warmupCount;
    final int measurementCount;
    final Duration minMeasurementDuration;
    final Duration maxMeasurementDuration;
    final Jvm jvm;

    private DatabaseLauncher( int warmupCount,
                              int measurementCount,
                              Duration minMeasurementDuration,
                              Duration maxMeasurementDuration,
                              Jvm jvm )
    {
        this.warmupCount = warmupCount;
        this.measurementCount = measurementCount;
        this.minMeasurementDuration = minMeasurementDuration;
        this.maxMeasurementDuration = maxMeasurementDuration;
        this.jvm = jvm;
    }

    /**
     * Specifies if the database will be started in a different process
     */
    public abstract boolean isDatabaseInDifferentProcess();

    /**
     * Performs any database initialization that needs to be done before launching benchmark execution fork.
     * E.g., starting a Neo4j server.
     *
     * @return initialized database connection
     */
    public abstract CONNECTION initDatabaseServer( Jvm jvm,
                                                   Store store,
                                                   Path neo4jConfigFile,
                                                   ForkDirectory forkDirectory,
                                                   List<String> additionalJvmArgs ) throws IOException, TimeoutException;

    /**
     * Benchmark tool command, to launch benchmark execution fork.
     *
     * @return command plus its arguments
     */
    public abstract List<String> toolArgs( Query query,
                                           CONNECTION connection,
                                           ForkDirectory forkDirectory,
                                           List<ProfilerType> profilerTypes,
                                           boolean isClientForked,
                                           Path neo4jConfigFile,
                                           Resources resources );

    /**
     * A hook for launcher to modify benchmark tool JVM arguments, if needed.
     *
     * @return returns modified JVM arguments or the same list
     */
    public abstract JvmArgs toolJvmArgs( JvmArgs clientJvmArgs );

    public static class EmbeddedLauncher extends DatabaseLauncher<EmbeddedLauncher.Connection>
    {
        private final Edition edition;

        public EmbeddedLauncher( Edition edition,
                                 int warmupCount,
                                 int measurementCount,
                                 Duration minMeasurementDuration,
                                 Duration maxMeasurementDuration,
                                 Jvm jvm )
        {
            super( warmupCount, measurementCount, minMeasurementDuration, maxMeasurementDuration, jvm );
            this.edition = edition;
        }

        @Override
        public boolean isDatabaseInDifferentProcess()
        {
            return false;
        }

        @Override
        public Connection initDatabaseServer( Jvm jvm,
                                              Store store,
                                              Path neo4jConfigFile,
                                              ForkDirectory forkDirectory,
                                              List<String> additionalJvmArgs )
        {
            return new Connection( store );
        }

        @Override
        public List<String> toolArgs( Query query,
                                      Connection connection,
                                      ForkDirectory forkDirectory,
                                      List<ProfilerType> profilerTypes,
                                      boolean isClientForked,
                                      Path neo4jConfigFile,
                                      Resources resources )
        {
            return RunSingleEmbeddedCommand.argsFor( query,
                                                     connection.store,
                                                     edition,
                                                     neo4jConfigFile,
                                                     forkDirectory,
                                                     internalProfilers( profilerTypes, isClientForked ),
                                                     warmupCount,
                                                     measurementCount,
                                                     minMeasurementDuration,
                                                     maxMeasurementDuration,
                                                     jvm,
                                                     resources.workDir() );
        }

        @Override
        public JvmArgs toolJvmArgs( JvmArgs jvmArgs )
        {
            return jvmArgs;
        }

        private static class Connection implements AutoCloseable
        {
            private final Store store;

            private Connection( Store store )
            {
                this.store = store;
            }

            @Override
            public void close()
            {
                // do nothing
            }
        }

    }

    public static class ServerLauncher extends DatabaseLauncher<ServerDatabase>
    {
        private final Path neo4jDir;

        public ServerLauncher( Path neo4jDir,
                               int warmupCount,
                               int measurementCount,
                               Duration minMeasurementDuration,
                               Duration maxMeasurementDuration,
                               Jvm jvm )
        {
            super( warmupCount, measurementCount, minMeasurementDuration, maxMeasurementDuration, jvm );
            this.neo4jDir = neo4jDir;
        }

        @Override
        public boolean isDatabaseInDifferentProcess()
        {
            return true;
        }

        @Override
        public ServerDatabase initDatabaseServer( Jvm jvm,
                                                  Store store,
                                                  Path neo4jConfigFile,
                                                  ForkDirectory forkDirectory,
                                                  List<String> additionalJvmArgs )
        {
            Redirect outputRedirect = Redirect.to( forkDirectory.pathFor( "neo4j-out.log" ).toFile() );
            Redirect errorRedirect = Redirect.to( forkDirectory.pathFor( "neo4j-error.log" ).toFile() );
            Neo4jConfigBuilder.fromFile( neo4jConfigFile )
                              .withSetting( BoltConnector.enabled, TRUE )
                              .addJvmArgs( additionalJvmArgs )
                              .writeToFile( neo4jConfigFile );
            Path copyLogsToOnClose = Paths.get( forkDirectory.toAbsolutePath() );
            return ServerDatabase.startServer( jvm, neo4jDir, store, neo4jConfigFile, outputRedirect, errorRedirect, copyLogsToOnClose );
        }

        @Override
        public List<String> toolArgs( Query query,
                                      ServerDatabase db,
                                      ForkDirectory forkDirectory,
                                      List<ProfilerType> profilerTypes,
                                      boolean isClientForked,
                                      Path neo4jConfigFile,
                                      Resources resources )
        {
            return RunSingleServerCommand.argsFor( query,
                                                   db.boltUri(),
                                                   db.pid(),
                                                   forkDirectory,
                                                   internalProfilers( profilerTypes, isClientForked ), // client profilers
                                                   profilerTypes, // server profilers
                                                   warmupCount,
                                                   measurementCount,
                                                   minMeasurementDuration,
                                                   maxMeasurementDuration,
                                                   jvm,
                                                   resources.workDir() );
        }

        @Override
        public JvmArgs toolJvmArgs( JvmArgs jvmArgs )
        {
            // for now we have hardcoded JVM memory sizes for client fork
            return jvmArgs
                    .set( "-Xmx2g" )
                    .set( "-Xms2g" );
        }
    }

}
