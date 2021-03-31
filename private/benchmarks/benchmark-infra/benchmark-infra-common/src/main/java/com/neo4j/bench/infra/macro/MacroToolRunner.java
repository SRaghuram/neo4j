/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.macro;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingToolRunner;
import com.neo4j.bench.infra.IgnoreProfilerFileFilter;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.ResultStoreCredentials;
import com.neo4j.bench.infra.Workspace;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;

public class MacroToolRunner implements BenchmarkingToolRunner<RunToolMacroWorkloadParams>
{
    private static final Logger LOG = LoggerFactory.getLogger( MacroToolRunner.class );

    @Override
    public void runTool( JobParams<RunToolMacroWorkloadParams> jobParams,
                         ArtifactStorage artifactStorage,
                         Path workspacePath,
                         Workspace artifactsWorkspace,
                         ResultStoreCredentials resultsStoreCredentials,
                         URI artifactBaseUri )
            throws IOException, InterruptedException, ArtifactStoreException
    {
        RunToolMacroWorkloadParams runToolMacroWorkloadParams =
                jobParams.benchmarkingRun().benchmarkingTool().getToolParameters();
        RunMacroWorkloadParams runMacroWorkloadParams = runToolMacroWorkloadParams.runMacroWorkloadParams();

        Path macroDir = workspacePath.resolve( "macro" );

        Files.setPosixFilePermissions( artifactsWorkspace.get( Workspace.RUN_SCRIPT ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

        Path neo4jConfigFile = artifactsWorkspace.get( Workspace.NEO4J_CONFIG );
        BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile );

            /*
            At this point the workspace looks as follow:

                workspace/
                    neo4j.conf
                    benchmark-infra-scheduler.jar
                    macro/
                        run-report-benchmarks.sh
                        target/
                            macro.jar
             */

        List<String> runReportCommands = new ArrayList<>();
        runReportCommands.add( "./run-report-benchmarks.sh" );
        runReportCommands.addAll(
                createRunReportArgs( runMacroWorkloadParams,
                                     jobParams.infraParams(),
                                     macroDir,
                                     runToolMacroWorkloadParams.dataSetUri(),
                                     neo4jConfigFile,
                                     resultsStoreCredentials,
                                     jobParams.benchmarkingRun().testRunId() ) );

        LOG.info( "starting run report benchmark process, {}", join( " ", runReportCommands ) );
        Process process = new ProcessBuilder( runReportCommands )
                .directory( macroDir.toFile() )
                .inheritIO()
                .start();

        int waitFor = process.waitFor();
        //if the benchmark process failed upload all artifacts
        FileFilter fileFilter = waitFor == 0 ? new IgnoreProfilerFileFilter() : TrueFileFilter.INSTANCE;
        Path workDir = macroDir.resolve( "execute_work_dir" );
        Files.createDirectories( workDir );
        Workspace resultsWorkspace = Workspace.create( workDir ).withFilesRecursively( fileFilter ).build();

        artifactStorage.uploadBuildArtifacts( artifactBaseUri.resolve( "results" ), resultsWorkspace );

        if ( waitFor != 0 )
        {
            throw new RuntimeException( format( "benchmark exited with code %d", waitFor ) );
        }
    }

    private static List<String> createRunReportArgs( RunMacroWorkloadParams runMacroWorkloadParams,
                                                     InfraParams infraParams,
                                                     Path workDir,
                                                     URI storeDir,
                                                     Path neo4jConfigFile,
                                                     ResultStoreCredentials resultStoreCredentials,
                                                     String testRunId )
    {
        ArrayList<String> strings = Lists.newArrayList( storeDir.toString(),
                                                        runMacroWorkloadParams.jvm().toAbsolutePath().toString(),
                                                        neo4jConfigFile.toAbsolutePath().toString(),
                                                        workDir.toAbsolutePath().toString(),
                                                        resultStoreCredentials.uri().toString(),
                                                        resultStoreCredentials.username(),
                                                        resultStoreCredentials.password(),
                                                        infraParams.errorReportingPolicy().name(),
                                                        "--test-run-id", testRunId,
                                                        "--recordings-base-uri", infraParams.recordingsBaseUri().toString() );
        strings.addAll( runMacroWorkloadParams.asArgs() );
        return strings;
    }
}
