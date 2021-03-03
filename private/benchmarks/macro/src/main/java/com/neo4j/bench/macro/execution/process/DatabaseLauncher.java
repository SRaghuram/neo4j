/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.process;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.cli.RunSingleEmbeddedCommand;
import com.neo4j.bench.macro.cli.RunSingleServerCommand;
import com.neo4j.bench.macro.execution.database.DelegatingServerDatabase;
import com.neo4j.bench.macro.execution.database.Neo4jServerDatabase;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.lang.ProcessBuilder.Redirect;

public abstract class DatabaseLauncher<CONNECTION extends AutoCloseable>
{

    private static final Logger LOG = LoggerFactory.getLogger( DatabaseLauncher.class );

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
                LOG.debug( warningMessage );
            }
            return nonExternalProfilerTypes;
        }
    }

    protected final MeasurementOptions measurementOptions;
    protected final Jvm jvm;
    private final Store originalStore;
    private final boolean copyStore;
    protected final Path neo4jConfigFile;
    protected final ForkDirectory forkDirectory;

    private DatabaseLauncher( MeasurementOptions measurementOptions,
                              Jvm jvm,
                              Store originalStore,
                              boolean copyStore,
                              Path neo4jConfigFile,
                              ForkDirectory forkDirectory )
    {
        this.measurementOptions = measurementOptions;
        this.jvm = jvm;
        this.originalStore = originalStore;
        this.copyStore = copyStore;
        this.neo4jConfigFile = neo4jConfigFile;
        this.forkDirectory = forkDirectory;
    }

    /**
     * Specifies if the database will be started in a different process
     */
    public abstract boolean isDatabaseInDifferentProcess();

    /**
     * Performs any database initialization that needs to be done before launching benchmark execution fork. E.g., starting a Neo4j server.
     *
     * @return initialized database connection
     */
    public abstract CONNECTION initDatabaseServer( List<String> additionalJvmArgs );

    /**
     * Benchmark tool command, to launch benchmark execution fork.
     *
     * @return command plus its arguments
     */
    public abstract List<String> toolArgs( Query query,
                                           CONNECTION connection,
                                           List<ProfilerType> profilerTypes,
                                           boolean isClientForked,
                                           Resources resources );

    /**
     * A hook for launcher to modify benchmark tool JVM arguments, if needed.
     *
     * @return returns modified JVM arguments or the same list
     */
    public abstract JvmArgs toolJvmArgs( JvmArgs clientJvmArgs );

    protected Store storeCopy()
    {
        return copyStore ? originalStore.makeTemporaryCopy() : originalStore;
    }

    public static class EmbeddedLauncher extends DatabaseLauncher<EmbeddedLauncher.Connection>
    {
        private final Edition edition;

        public EmbeddedLauncher( Edition edition,
                                 MeasurementOptions measurementOptions,
                                 Jvm jvm,
                                 Store originalStore,
                                 boolean copyStore,
                                 Path neo4jConfigFile,
                                 ForkDirectory forkDirectory )
        {
            super( measurementOptions, jvm, originalStore, copyStore, neo4jConfigFile, forkDirectory );
            this.edition = edition;
        }

        @Override
        public boolean isDatabaseInDifferentProcess()
        {
            return false;
        }

        @Override
        public Connection initDatabaseServer( List<String> additionalJvmArgs )
        {
            return new Connection( storeCopy() );
        }

        @Override
        public List<String> toolArgs( Query query,
                                      Connection connection,
                                      List<ProfilerType> profilerTypes,
                                      boolean isClientForked,
                                      Resources resources )
        {
            return RunSingleEmbeddedCommand.argsFor( query,
                                                     connection.store,
                                                     edition,
                                                     neo4jConfigFile,
                                                     forkDirectory,
                                                     internalProfilers( profilerTypes, isClientForked ),
                                                     measurementOptions,
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
                store.close();
            }
        }
    }

    public static class ServerLauncher extends DatabaseLauncher<ServerDatabase>
    {
        private final Path neo4jDir;

        public ServerLauncher( Path neo4jDir,
                               MeasurementOptions measurementOptions,
                               Jvm jvm,
                               Store originalStore,
                               boolean copyStore,
                               Path neo4jConfigFile,
                               ForkDirectory forkDirectory )
        {
            super( measurementOptions, jvm, originalStore, copyStore, neo4jConfigFile, forkDirectory );
            this.neo4jDir = neo4jDir;
        }

        @Override
        public boolean isDatabaseInDifferentProcess()
        {
            return true;
        }

        @Override
        public ServerDatabase initDatabaseServer( List<String> additionalJvmArgs )
        {
            Redirect outputRedirect = Redirect.to( forkDirectory.pathFor( "neo4j-out.log" ).toFile() );
            Redirect errorRedirect = Redirect.to( forkDirectory.pathFor( "neo4j-error.log" ).toFile() );
            Neo4jConfigBuilder.fromFile( neo4jConfigFile )
                              .addJvmArgs( additionalJvmArgs )
                              .writeToFile( neo4jConfigFile );
            Path copyLogsToOnClose = Paths.get( forkDirectory.toAbsolutePath() );
            Store store = storeCopy();
            ServerDatabase serverDatabase = Neo4jServerDatabase.startServer( jvm,
                                                                             neo4jDir,
                                                                             store,
                                                                             neo4jConfigFile,
                                                                             outputRedirect,
                                                                             errorRedirect,
                                                                             copyLogsToOnClose );
            return new DelegatingServerDatabase( serverDatabase, store::close );
        }

        @Override
        public List<String> toolArgs( Query query,
                                      ServerDatabase db,
                                      List<ProfilerType> profilerTypes,
                                      boolean isClientForked,
                                      Resources resources )
        {
            return RunSingleServerCommand.argsFor( query,
                                                   db.boltUri(),
                                                   db.pid(),
                                                   forkDirectory,
                                                   internalProfilers( profilerTypes, isClientForked ), // client profilers
                                                   profilerTypes, // server profilers
                                                   measurementOptions,
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
