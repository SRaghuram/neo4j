/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingToolRunner;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Extractor;
import com.neo4j.bench.infra.IgnoreProfilerFileFilter;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import static com.neo4j.bench.common.tool.macro.DeploymentModes.SERVER;
import static java.lang.String.format;
import static java.lang.String.join;

public class MacroToolRunner implements BenchmarkingToolRunner<RunToolMacroWorkloadParams>
{
    private static final Logger LOG = LoggerFactory.getLogger( MacroToolRunner.class );

    @Override
    public void runTool( RunToolMacroWorkloadParams runToolMacroWorkloadParams,
                         ArtifactStorage artifactStorage,
                         Path workspacePath,
                         Workspace artifactsWorkspace,
                         InfraParams infraParams,
                         String resultsStorePassword,
                         String batchJobId,
                         URI artifactBaseUri )
            throws IOException, InterruptedException, ArtifactStoreException
    {

        RunMacroWorkloadParams runMacroWorkloadParams = runToolMacroWorkloadParams.runMacroWorkloadParams();

        Path macroDir = workspacePath.resolve( "macro" );

        // download & extract dataset
        Version neo4jVersion = runMacroWorkloadParams.neo4jVersion();
        Dataset dataset = artifactStorage.downloadDataset( neo4jVersion.minorVersion(), runToolMacroWorkloadParams.storeName() );
        dataset.extractInto( macroDir );

        Files.setPosixFilePermissions( artifactsWorkspace.get( Workspace.RUN_SCRIPT ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

        Path neo4jConfigFile = artifactsWorkspace.get( Workspace.NEO4J_CONFIG );
        BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile );

        Path workDir = macroDir.resolve( "execute_work_dir" );
        Files.createDirectories( workDir );

        Path storeDir = macroDir.resolve( runToolMacroWorkloadParams.storeName() );

        // Unzip the neo4jJar
        if ( SERVER.equals( runMacroWorkloadParams.deployment().deploymentModes() ) )
        {
            Path neo4jTar = artifactsWorkspace.get( Workspace.NEO4J_ARCHIVE );
            Extractor.extract( workspacePath.resolve( macroDir ), Files.newInputStream( neo4jTar ) );
        }
            /*
            At this point the workspace looks as follow:

                workspace/
                    neo4j.conf
                    benchmark-infra-scheduler.jar
                    neo4j-{edition}-{version}-unix.tar.gz
                    macro/
                        neo4j-{edition}-{version}/
                        run-report-benchmarks.sh
                        {store_name}/
                            graph.db/
                        target/
                            macro.jar
                        execute_work_dir/
                            results.json                        // not yet created
             */

        List<String> runReportCommands = new ArrayList<>();
        runReportCommands.add( "./run-report-benchmarks.sh" );
        runReportCommands.addAll(
                createRunReportArgs( runMacroWorkloadParams, infraParams, workDir, storeDir, neo4jConfigFile, batchJobId, resultsStorePassword ) );

        LOG.info( "starting run report benchmark process, {}", join( " ", runReportCommands ) );
        Process process = new ProcessBuilder( runReportCommands )
                .directory( macroDir.toFile() )
                .inheritIO()
                .start();

        int waitFor = process.waitFor();

        Workspace resultsWorkspace = Workspace.create( workDir ).withFilesRecursively( new IgnoreProfilerFileFilter() ).build();

        artifactStorage.uploadBuildArtifacts( artifactBaseUri.resolve( "results" ), resultsWorkspace );

        if ( waitFor != 0 )
        {
            throw new RuntimeException( format( "benchmark exited with code %d", waitFor ) );
        }
    }

    private static List<String> createRunReportArgs( RunMacroWorkloadParams runMacroWorkloadParams,
                                                     InfraParams infraParams,
                                                     Path workDir,
                                                     Path storeDir,
                                                     Path neo4jConfigFile,
                                                     String batchJobId,
                                                     String resultsStorePassword )
    {
        return Lists.newArrayList( runMacroWorkloadParams.workloadName(),
                                   storeDir.toAbsolutePath().toString(),
                                   Integer.toString( runMacroWorkloadParams.warmupCount() ),
                                   Integer.toString( runMacroWorkloadParams.measurementCount() ),
                                   runMacroWorkloadParams.neo4jEdition().name(),
                                   runMacroWorkloadParams.jvm().toAbsolutePath().toString(),
                                   neo4jConfigFile.toAbsolutePath().toString(),
                                   workDir.toAbsolutePath().toString(),
                                   ParameterizedProfiler.serialize( runMacroWorkloadParams.profilers() ),
                                   Integer.toString( runMacroWorkloadParams.measurementForkCount() ),
                                   runMacroWorkloadParams.unit().name(),
                                   infraParams.resultsStoreUri().toString(),
                                   infraParams.resultsStoreUsername(),
                                   resultsStorePassword,
                                   runMacroWorkloadParams.neo4jCommit(),
                                   runMacroWorkloadParams.neo4jVersion().patchVersion(),
                                   runMacroWorkloadParams.neo4jBranch(),
                                   runMacroWorkloadParams.neo4jBranchOwner(),
                                   runMacroWorkloadParams.toolCommit(),
                                   runMacroWorkloadParams.toolOwner(),
                                   runMacroWorkloadParams.toolBranch(),
                                   Long.toString( runMacroWorkloadParams.teamcityBuild() ),
                                   Long.toString( runMacroWorkloadParams.parentBuild() ),
                                   runMacroWorkloadParams.executionMode().name(),
                                   runMacroWorkloadParams.jvmArgs().toArgsString(),
                                   Boolean.toString( runMacroWorkloadParams.isRecreateSchema() ),
                                   runMacroWorkloadParams.planner().name(),
                                   runMacroWorkloadParams.runtime().name(),
                                   runMacroWorkloadParams.triggeredBy(),
                                   infraParams.errorReportingPolicy().name(),
                                   runMacroWorkloadParams.deployment().parsableValue(),
                                   String.join( ",", runMacroWorkloadParams.queryNames() ),
                                   RunMacroWorkloadParams.CMD_BATCH_JOB_ID, batchJobId );
    }
}
