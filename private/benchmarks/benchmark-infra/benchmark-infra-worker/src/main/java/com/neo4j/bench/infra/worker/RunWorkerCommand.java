package com.neo4j.bench.infra.worker;
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */

import com.amazonaws.SdkClientException;
import com.github.rvesse.airline.annotations.Command;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.commands.BaseInfraCommand;
import com.neo4j.bench.infra.commands.InfraParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;

@Command( name = "run-worker" )
public class RunWorkerCommand extends BaseInfraCommand
{
    private static final Logger LOG = LoggerFactory.getLogger( RunWorkerCommand.class );

    @Override
    protected void doRunInfra( RunWorkloadParams runWorkloadParams, InfraParams infraParams )
    {
        try
        {
            Path macroDir = infraParams.workspaceDir().resolve( "macro" );

            AWSS3ArtifactStorage artifactStorage;
            if ( !infraParams.hasAwsCredentials() )
            {
                artifactStorage = AWSS3ArtifactStorage.create( infraParams.awsRegion() );
            }
            else
            {
                artifactStorage = AWSS3ArtifactStorage.create( infraParams.awsRegion(), infraParams.awsKey(), infraParams.awsSecret() );
            }

            // download & extract dataset
            Version neo4jVersion = runWorkloadParams.neo4jVersion();
            Dataset dataset = artifactStorage.downloadDataset( neo4jVersion.minorVersion(), infraParams.storeName() );
            dataset.extractInto( macroDir );

            // download artifacts
            artifactStorage.downloadBuildArtifacts( infraParams.workspaceDir(), infraParams.artifactBaseUri() );
            Files.setPosixFilePermissions( macroDir.resolve( "run-report-benchmarks.sh" ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

            Workspace.assertMacroWorkspace( infraParams.workspaceDir(), runWorkloadParams.neo4jEdition(),
                                            neo4jVersion );

            Path neo4jConfigFile = infraParams.workspaceDir().resolve( "neo4j.conf" );
            BenchmarkUtil.assertFileNotEmpty( neo4jConfigFile );

            Path workDir = macroDir.resolve( "execute_work_dir" );
            Files.createDirectories( workDir );

            Path storeDir = macroDir.resolve( infraParams.storeName() );
            Path resultsJson = workDir.resolve( "results.json" );

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
                        work/
                            results.json                        // not yet created
             */

            List<String> runReportCommands = new ArrayList<>();
            runReportCommands.add( "./run-report-benchmarks.sh" );
            runReportCommands.addAll( createRunReportArgs( runWorkloadParams, infraParams, workDir, storeDir, neo4jConfigFile, resultsJson ) );

            LOG.info( "starting run report benchmark process, {}", join( " ", runReportCommands ) );
            Process process = new ProcessBuilder( runReportCommands )
                    .directory( macroDir.toFile() )
                    .inheritIO()
                    .start();

            int waitFor = process.waitFor();
            if ( waitFor != 0 )
            {
                throw new RuntimeException( format( "benchmark exited with code %d", waitFor ) );
            }
        }
        catch ( SdkClientException | IOException | InterruptedException | ArtifactStoreException e )
        {
            throw new RuntimeException( "fatal error in worker", e );
        }
    }

    private static List<String> createRunReportArgs( RunWorkloadParams runWorkloadParams,
                                                     InfraParams infraParams,
                                                     Path workDir,
                                                     Path storeDir,
                                                     Path neo4jConfigFile,
                                                     Path resultsJson )
    {
        return Lists.newArrayList( runWorkloadParams.workloadName(),
                                   storeDir.toAbsolutePath().toString(),
                                   Integer.toString( runWorkloadParams.warmupCount() ),
                                   Integer.toString( runWorkloadParams.measurementCount() ),
                                   runWorkloadParams.neo4jEdition().name(),
                                   runWorkloadParams.jvm().toAbsolutePath().toString(),
                                   neo4jConfigFile.toAbsolutePath().toString(),
                                   workDir.toAbsolutePath().toString(),
                                   ProfilerType.serializeProfilers( runWorkloadParams.profilers() ),
                                   Integer.toString( runWorkloadParams.measurementForkCount() ),
                                   resultsJson.toAbsolutePath().toString(),
                                   runWorkloadParams.unit().name(),
                                   infraParams.resultsStoreUri().toString(),
                                   infraParams.resultsStoreUsername(),
                                   infraParams.resultsStorePassword(),
                                   runWorkloadParams.neo4jCommit(),
                                   runWorkloadParams.neo4jVersion().patchVersion(),
                                   runWorkloadParams.neo4jBranch(),
                                   runWorkloadParams.neo4jBranchOwner(),
                                   runWorkloadParams.toolCommit(),
                                   runWorkloadParams.toolOwner(),
                                   runWorkloadParams.toolBranch(),
                                   Long.toString( runWorkloadParams.teamcityBuild() ),
                                   Long.toString( runWorkloadParams.parentBuild() ),
                                   runWorkloadParams.executionMode().name(),
                                   String.join( " ", runWorkloadParams.jvmArgs() ),
                                   Boolean.toString( runWorkloadParams.isRecreateSchema() ),
                                   runWorkloadParams.planner().name(),
                                   runWorkloadParams.runtime().name(),
                                   runWorkloadParams.triggeredBy(),
                                   runWorkloadParams.errorPolicy().name(),
                                   runWorkloadParams.deployment().parsableValue() );
    }
}
