/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.command.ResultsStoreArgs;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.micro.RunMicroWorkloadParams;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingRun;
import com.neo4j.bench.infra.BenchmarkingTool;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.URIHelper;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.micro.MicroToolRunner;
import com.neo4j.bench.infra.scheduler.BenchmarkJobScheduler;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;
import javax.inject.Inject;

import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_ERROR_POLICY;
import static java.lang.String.format;

@Command( name = "schedule" )
public class ScheduleMicroCommand extends BaseRunWorkloadCommand
{
    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_JOB_QUEUE,
            title = "AWS Batch Job Queue Name",
            description = "job queue name in CloudFormation stack" )
    @Required
    private String jobQueue;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_JOB_DEFINITION,
            title = "AWS Batch Job Definition Name",
            description = "job definition in CloudFormation stack" )
    private String jobDefinition = "macro-benchmark-job-definition";

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_BATCH_STACK,
            title = "AWS Batch Stack Name",
            description = "name of stack in CloudFormation" )
    private String batchStack = "benchmarking";

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_WORKSPACE_DIR,
            description = "Local directory containing artifacts to be uploaded to S3, which the worker requires",
            title = "Local workspace" )
    private File workspaceDir;

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

    @Option( type = OptionType.COMMAND,
            name = {InfraParams.CMD_RESULTS_STORE_PASSWORD_SECRET_NAME},
            description = "Secret name in AWS Secrets Manager with password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password Secret Name" )
    @Required
    private String resultsStorePasswordSecretName;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_AWS_SECRET,
            title = "AWS Secret" )
    private String awsSecret;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_AWS_KEY,
            title = "AWS Key" )
    private String awsKey;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_AWS_REGION,
            title = "AWS Region" )
    private String awsRegion = "eu-north-1";

    @Option( type = OptionType.COMMAND,
            name = {CMD_ERROR_POLICY},
            description = "Specify if execution should terminate on error, or skip and continue",
            title = "Error handling policy" )
    private ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.IGNORE;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_ARTIFACT_BASE_URI,
            description = "Location of worker jar and other artifacts needed " +
                          "(e.g., s3://benchmarking.neo4j.com/artifacts/micro/<triggered_by>/<build_id>/<uuid>) in S3",
            title = "Location of worker jar" )
    @Required
    private URI artifactBaseUri;

    private static String getJobName( String tool, String version, String triggered )
    {
        return format( "%s-%s-%s", tool, version, triggered );
    }

    @Override
    protected final void doRun( RunMicroWorkloadParams runMicroWorkloadParams )
    {
        AWSCredentials awsCredentials = new AWSCredentials( awsKey, awsSecret, awsRegion );
        try ( ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( awsCredentials ) )
        {
            Path workspacePath = workspaceDir.toPath().toAbsolutePath();
            File jobParameterJson = workspacePath.resolve( Workspace.JOB_PARAMETERS_JSON ).toFile();
            jobParameterJson.createNewFile();
            Workspace workspace = Workspace.defaultMicroWorkspace( workspacePath );
            workspace.assertArtifactsExist();

            UUID uuid = UUID.randomUUID();
            URI artifactBaseWorkloadURI = artifactBaseUri.resolve( URIHelper.toURIPart( runMicroWorkloadParams.triggeredBy() ) )
                                                         .resolve( URIHelper.toURIPart( Long.toString( runMicroWorkloadParams.build() ) ) )
                                                         .resolve( URIHelper.toURIPart( uuid.toString() ) );
            URI artifactWorkerUri = artifactBaseWorkloadURI.resolve( "benchmark-infra-worker.jar" );
            InfraParams infraParams = new InfraParams( awsCredentials,
                                                       resultsStoreArgs.resultsStoreUsername(),
                                                       resultsStorePasswordSecretName,
                                                       resultsStoreArgs.resultsStoreUri(),
                                                       artifactBaseWorkloadURI,
                                                       errorReportingPolicy,
                                                       workspace );
            String testRunId = UUID.randomUUID().toString();
            JobParams<MicroToolRunner> jobParams = new JobParams<>( infraParams,
                                                                    new BenchmarkingRun(
                                                                            new BenchmarkingTool<>( MicroToolRunner.class, runMicroWorkloadParams ),
                                                                            testRunId ) );

            String jobName = getJobName( "micro",
                                         runMicroWorkloadParams.neo4jVersion().toString(),
                                         runMicroWorkloadParams.triggeredBy() );

            BenchmarkJobScheduler benchmarkJobScheduler = BenchmarkJobScheduler.create( jobQueue, jobDefinition, batchStack, awsCredentials, artifactStorage );

            benchmarkJobScheduler.scheduleBenchmarkJob( jobName, jobParams, workspace, artifactWorkerUri );

            benchmarkJobScheduler.awaitFinished();
        }
        catch ( ArtifactStoreException | IOException e )
        {
            throw new RuntimeException( "failed to schedule benchmarking job", e );
        }
    }
}
