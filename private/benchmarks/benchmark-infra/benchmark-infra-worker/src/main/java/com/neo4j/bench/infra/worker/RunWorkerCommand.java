package com.neo4j.bench.infra.worker;
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */

import com.amazonaws.SdkClientException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.PasswordManager;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSPasswordManager;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.commands.InfraParams;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.join;

@Command( name = "run-worker" )
public class RunWorkerCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( RunWorkerCommand.class );

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_AWS_REGION,
             title = "AWS Region" )
    private String awsRegion = "eu-north-1";

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_ARTIFACT_BASE_URI,
             description = "Location of worker jar and other artifacts needed (e.g., s3://benchmarking.neo4j.com/artifacts/<build_id>/) in S3",
             title = "Location of build artifacts" )
    @Required
    private URI artifactBaseUri;

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_WORKSPACE_DIR,
             description = "Local directory containing artifacts to be uploaded to S3, which the worker requires",
             title = "Local workspace" )
    @Required
    private File workspaceDir;

    @Option( type = OptionType.COMMAND,
             name = RunWorkloadParams.CMD_BATCH_JOB_ID,
             title = "AWS Batch JOB ID" )
    @Required
    private String batchJobId = "";

    @Override
    public void run()
    {
        try
        {
            AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( awsRegion );

            // download artifacts
            Path workspacePath = workspaceDir.toPath();
            Workspace artifactsWorkspace = artifactStorage.downloadBuildArtifacts( workspacePath, artifactBaseUri );

            Path jobParametersJson = artifactsWorkspace.get( Workspace.JOB_PARAMETERS_JSON );
            JobParams jobParams = JobParams.fromJson( Files.lines( jobParametersJson ).collect( Collectors.joining( "\n" ) ) );

            InfraParams infraParams = jobParams.infraParams();
            RunWorkloadParams runWorkloadParams = jobParams.runWorkloadParams();

            // fetch result db password
            PasswordManager awsSecretsManager = AWSPasswordManager.create( infraParams.awsRegion() );
            String resultsStorePassword = awsSecretsManager.getSecret( infraParams.resultsStorePasswordSecretName() );

            Workspace.assertMacroWorkspace( artifactsWorkspace, runWorkloadParams.neo4jVersion(), runWorkloadParams.neo4jEdition() );

            Path macroDir = workspacePath.resolve( "macro" );

            // download & extract dataset
            Version neo4jVersion = runWorkloadParams.neo4jVersion();
            Dataset dataset = artifactStorage.downloadDataset( neo4jVersion.minorVersion(), infraParams.storeName() );
            dataset.extractInto( macroDir );

            Files.setPosixFilePermissions( macroDir.resolve( "run-report-benchmarks.sh" ), PosixFilePermissions.fromString( "r-xr-xr-x" ) );

            Path neo4jConfigFile = workspacePath.resolve( "neo4j.conf" );
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
                        execute_work_dir/
                            results.json                        // not yet created
             */

            List<String> runReportCommands = new ArrayList<>();
            runReportCommands.add( "./run-report-benchmarks.sh" );
            runReportCommands.addAll(
                    createRunReportArgs( runWorkloadParams, infraParams, workDir, storeDir, neo4jConfigFile, resultsJson, batchJobId, resultsStorePassword ) );

            LOG.info( "starting run report benchmark process, {}", join( " ", runReportCommands ) );
            Process process = new ProcessBuilder( runReportCommands )
                    .directory( macroDir.toFile() )
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
                                                     Path resultsJson,
                                                     String batchJobId,
                                                     String resultsStorePassword )
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
                                   resultsStorePassword,
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
                                   runWorkloadParams.jvmArgs().toArgsString(),
                                   Boolean.toString( runWorkloadParams.isRecreateSchema() ),
                                   runWorkloadParams.planner().name(),
                                   runWorkloadParams.runtime().name(),
                                   runWorkloadParams.triggeredBy(),
                                   infraParams.errorReportingPolicy().name(),
                                   runWorkloadParams.deployment().parsableValue(),
                                   batchJobId );
    }
}
