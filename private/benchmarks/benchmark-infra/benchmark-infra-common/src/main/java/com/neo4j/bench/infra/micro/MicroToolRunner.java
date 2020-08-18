/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.micro;

import com.google.common.collect.Lists;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.micro.RunMicroWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingToolRunner;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.Workspace;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;

public class MicroToolRunner implements BenchmarkingToolRunner<RunMicroWorkloadParams>
{
    private static final Logger LOG = LoggerFactory.getLogger( MicroToolRunner.class );

    @Override
    public void runTool( JobParams<RunMicroWorkloadParams> jobParams,
                         ArtifactStorage artifactStorage,
                         Path workspacePath,
                         Workspace artifactsWorkspace,
                         String resultsStorePassword,
                         URI artifactBaseUri )
            throws IOException, InterruptedException, ArtifactStoreException
    {

        Workspace.assertMicroWorkspace( artifactsWorkspace );

        Path microDir = workspacePath.resolve( "micro" );

        Files.setPosixFilePermissions( microDir.resolve( "run-report-benchmarks.sh" ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

        Path neo4jConfigFile = workspacePath.resolve( "neo4j.conf" );
        BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile );

        Path configFile = workspacePath.resolve( "config" );
        BenchmarkUtil.assertFileNotEmpty( configFile );

        Path workDir = workspacePath.resolve( "execute_work_dir" );
        Files.createDirectories( workDir );

        /*
            At this point the workspace looks as follow:

                workspace/
                    neo4j.conf
                    benchmark-infra-scheduler.jar
                    neo4j-{edition}-{version}-unix.tar.gz
                    macro/
                        run-report-benchmarks.sh
                        {store_name}/
                            graph.db/
                        target/
                            macro.jar
                        execute_work_dir/
                            results.json                        // not yet created
             */
        RunMicroWorkloadParams runMicroWorkloadParams = jobParams.benchmarkingRun().benchmarkingTool().getToolParameters();
        List<String> runReportCommands = createRunReportArgs( runMicroWorkloadParams,
                                                              jobParams.infraParams(),
                                                              neo4jConfigFile,
                                                              configFile,
                                                              workDir,
                                                              resultsStorePassword );

        LOG.info( "starting run report benchmark process, {}", join( " ", runReportCommands ) );
        Process process = new ProcessBuilder( runReportCommands )
                .directory( microDir.toFile() )
                .inheritIO()
                .start();

        int waitFor = process.waitFor();

        Workspace resultsWorkspace = Workspace.create( workDir ).withFilesRecursively( TrueFileFilter.INSTANCE ).build();

        artifactStorage.uploadBuildArtifacts( artifactBaseUri.resolve( "results" ), resultsWorkspace );

        if ( waitFor != 0 )
        {
            throw new RuntimeException( format( "benchmark exited with code %d", waitFor ) );
        }
    }

    private static List<String> createRunReportArgs( RunMicroWorkloadParams runMicroWorkloadParams,
                                                     InfraParams infraParams,
                                                     Path neo4jConfigFile,
                                                     Path configFile,
                                                     Path workDir,
                                                     String resultsStorePassword )
    {
        return Lists.newArrayList(
                "./run-report-benchmarks.sh",
                runMicroWorkloadParams.neo4jVersion().toString(),
                runMicroWorkloadParams.neo4jCommit(),
                runMicroWorkloadParams.neo4jBranch(),
                runMicroWorkloadParams.neo4jBranchOwner(),
                runMicroWorkloadParams.toolBranch(),
                runMicroWorkloadParams.toolBranchOwner(),
                runMicroWorkloadParams.toolCommit(),
                infraParams.resultsStoreUri().toString(),
                infraParams.resultsStoreUsername(),
                resultsStorePassword,
                configFile.toString(),
                Long.toString( runMicroWorkloadParams.build() ),
                Long.toString( runMicroWorkloadParams.parentBuild() ),
                runMicroWorkloadParams.jvmArgs().toArgsString(),
                runMicroWorkloadParams.jmhArgs(),
                neo4jConfigFile.toString(),
                runMicroWorkloadParams.jvm().toString(),
                ProfilerType.serializeProfilers( runMicroWorkloadParams.profilers() ),
                runMicroWorkloadParams.triggeredBy(),
                workDir.toString()
        );
    }
}
