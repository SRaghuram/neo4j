/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.profiling.ExternalProfiler;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.assist.ExternalProfilerAssist;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.agent.WorkspaceStorage;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.execution.process.DatabaseLauncher;
import com.neo4j.bench.common.tool.macro.MeasurementParams;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.process.JvmArgs;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public abstract class Neo4jDeployment<DEPLOYMENT extends Deployment, CONNECTION extends AutoCloseable>
{
    public static Neo4jDeployment workspace( Deployment deployment,
                                             Edition edition,
                                             MeasurementParams measurementParams,
                                             Jvm jvm,
                                             URI datasetUri,
                                             Path workDir,
                                             WorkspaceStorage workspaceStorage )
    {
        Path dataset = workspaceStorage.resolve( datasetUri );

        return from( resolveProduct( workspaceStorage, deployment ),
                     edition,
                     measurementParams,
                     jvm,
                     dataset,
                     workDir );
    }

    private static Deployment resolveProduct( WorkspaceStorage workspaceStorage, Deployment deployment )
    {
        if ( deployment instanceof Deployment.Server )
        {
            Deployment.Server server = (Deployment.Server) deployment;
            String neo4jDir = workspaceStorage.resolve( server.uri() ).toAbsolutePath().toString();
            Deployment resolved = Deployment.server( neo4jDir );
            resolved.assertExists();
            return resolved;
        }
        else
        {
            return deployment;
        }
    }

    public static Neo4jDeployment from( Deployment deployment,
                                        Edition edition,
                                        MeasurementParams measurementParams,
                                        Jvm jvm,
                                        Path storeDir,
                                        Path workDir )
    {
        if ( deployment instanceof Deployment.Embedded )
        {
            return new EmbeddedNeo4jDeployment( (Deployment.Embedded) deployment, edition, measurementParams, jvm, Neo4jStore.createFrom( storeDir ),
                                                workDir );
        }
        else if ( deployment instanceof Deployment.Server )
        {
            return new ServerNeo4jDeployment( (Deployment.Server) deployment, measurementParams, jvm, Neo4jStore.createFrom( storeDir ), workDir );
        }
        else
        {
            throw new RuntimeException( "Invalid deployment mode value: " + deployment.getClass().getName() + " (" + deployment.toString() + ")" );
        }
    }

    private final DEPLOYMENT deployment;
    protected final MeasurementParams measurementParams;
    protected final Jvm jvm;
    protected final Store originalStore;
    protected final Path workDir;

    private Neo4jDeployment( DEPLOYMENT deployment, MeasurementParams measurementParams, Jvm jvm, Store originalStore, Path workDir )
    {
        this.deployment = deployment;
        this.measurementParams = measurementParams;
        this.jvm = jvm;
        this.originalStore = originalStore;
        this.workDir = workDir;
    }

    public final DEPLOYMENT deployment()
    {
        return deployment;
    }

    public final Store originalStore()
    {
        return originalStore;
    }

    @Override
    public String toString()
    {
        return deployment().toString();
    }

    public abstract DatabaseLauncher<CONNECTION> launcherFor( Query query,
                                                              Path neo4jConfigFile,
                                                              ForkDirectory forkDirectory,
                                                              JvmArgs jvmArgs,
                                                              List<ParameterizedProfiler> parameterizedProfilers );

    private static class EmbeddedNeo4jDeployment extends Neo4jDeployment<Deployment.Embedded,DatabaseLauncher.EmbeddedLauncher.Connection>
    {
        private final Edition edition;

        private EmbeddedNeo4jDeployment( Deployment.Embedded deployment,
                                         Edition edition,
                                         MeasurementParams measurementParams,
                                         Jvm jvm,
                                         Store originalStore,
                                         Path workDir )
        {
            super( deployment, measurementParams, jvm, originalStore, workDir );
            this.edition = edition;
        }

        @Override
        public DatabaseLauncher<DatabaseLauncher.EmbeddedLauncher.Connection> launcherFor( Query query,
                                                                                           Path neo4jConfigFile,
                                                                                           ForkDirectory forkDirectory,
                                                                                           JvmArgs jvmArgs,
                                                                                           List<ParameterizedProfiler> parameterizedProfilers )
        {
            List<ExternalProfiler> externalProfilers = ProfilerType.createExternalProfilers( ParameterizedProfiler.profilerTypes( parameterizedProfilers ) );
            ExternalProfilerAssist clientAssist = ExternalProfilerAssist.create( externalProfilers,
                                                                                 forkDirectory,
                                                                                 query.benchmarkGroup(),
                                                                                 query.benchmark(),
                                                                                 Collections.emptySet(),
                                                                                 jvm,
                                                                                 Parameters.NONE );
            List<ProfilerType> internalProfilers = ParameterizedProfiler.profilerTypes( ParameterizedProfiler.internalProfilers( parameterizedProfilers ) );
            return new DatabaseLauncher.EmbeddedLauncher( edition,
                                                          measurementParams,
                                                          jvm,
                                                          originalStore,
                                                          workDir,
                                                          query,
                                                          neo4jConfigFile,
                                                          forkDirectory,
                                                          jvmArgs,
                                                          clientAssist,
                                                          internalProfilers );
        }
    }

    private static class ServerNeo4jDeployment extends Neo4jDeployment<Deployment.Server,ServerDatabase>
    {
        private ServerNeo4jDeployment( Deployment.Server deployment,
                                       MeasurementParams measurementParams,
                                       Jvm jvm,
                                       Store originalStore,
                                       Path workDir )
        {
            super( deployment, measurementParams, jvm, originalStore, workDir );
        }

        @Override
        public DatabaseLauncher<ServerDatabase> launcherFor( Query query,
                                                             Path neo4jConfigFile,
                                                             ForkDirectory forkDirectory,
                                                             JvmArgs jvmArgs,
                                                             List<ParameterizedProfiler> parameterizedProfilers )
        {
            List<ExternalProfiler> externalProfilers = ProfilerType.createExternalProfilers( ParameterizedProfiler.profilerTypes( parameterizedProfilers ) );
            ExternalProfilerAssist clientAssist = ExternalProfilerAssist.create( externalProfilers,
                                                                                 forkDirectory,
                                                                                 query.benchmarkGroup(),
                                                                                 query.benchmark(),
                                                                                 Collections.emptySet(),
                                                                                 jvm,
                                                                                 Parameters.CLIENT );
            ExternalProfilerAssist serverAssist = ExternalProfilerAssist.create( externalProfilers,
                                                                                 forkDirectory,
                                                                                 query.benchmarkGroup(),
                                                                                 query.benchmark(),
                                                                                 Collections.emptySet(),
                                                                                 jvm,
                                                                                 Parameters.SERVER );
            List<ProfilerType> internalProfilers = ParameterizedProfiler.profilerTypes( ParameterizedProfiler.internalProfilers( parameterizedProfilers ) );
            return new DatabaseLauncher.ServerLauncher( deployment().path(),
                                                        measurementParams,
                                                        jvm,
                                                        originalStore,
                                                        workDir,
                                                        query,
                                                        neo4jConfigFile,
                                                        forkDirectory,
                                                        jvmArgs,
                                                        clientAssist,
                                                        serverAssist,
                                                        internalProfilers );
        }
    }
}
