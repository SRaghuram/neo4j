/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.BaseRunWorkloadCommand;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSBatchJobScheduler;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.commands.InfraParams;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_ERROR_POLICY;
import static java.util.stream.Collectors.joining;

@Command( name = "schedule-macro" )
public class ScheduleMacroCommand extends BaseRunWorkloadCommand
{
    private static final Logger LOG = LoggerFactory.getLogger( ScheduleMacroCommand.class );

    // job queue name in CloudFormation stack
    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_JOB_QUEUE,
             title = "AWS Batch Job Queue Name",
             description = "job queue name in CloudFormation stack" )
    private String jobQueue = "macro-benchmark-run-queue";

    // job definition in CloudFormation stack
    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_JOB_DEFINITION,
             title = "AWS Batch Job Definition Name",
             description = "job definition in CloudFormation stack" )
    private String jobDefinition = "macro-benchmark-job-definition";

    // name of stack in CloudFormation
    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_BATCH_STACK,
             title = "AWS Batch Stack Name",
             description = "name of stack in CloudFormation" )
    private String batchStack = "benchmarking";

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_ARTIFACT_WORKER_URI,
             description = "Location of worker jar(e.g., s3://benchmarking.neo4j.com/artifacts/<build_id>/worker.jar) in S3",
             title = "Worker Artifact URI" )
    private URI artifactWorkerUri;

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_WORKSPACE_DIR,
             description = "Local directory containing artifacts to be uploaded to S3, which the worker requires",
             title = "Local workspace" )
    private File workspaceDir;
    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;
    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_PASSWORD_SECRET_NAME},
             description = "Secret name in AWS Secrets Manager with password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password Secret Name" )
    @Required
    private String resultsStorePasswordSecretName;
    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;
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
             name = {InfraParams.CMD_DB_NAME},
             description = "Store name, e.g., for s3://benchmarking.neo4j.com/datasets/macro/3.5-enterprise-datasets/pokec.tgz it would be 'pokec'",
             title = "Store name" )
    @Required
    private String storeName;
    @Option( type = OptionType.COMMAND,
             name = {CMD_ERROR_POLICY},
             description = "Specify if execution should terminate on error, or skip and continue",
             title = "Error handling policy" )
    private ErrorReportingPolicy errorReportingPolicy = ErrorReportingPolicy.IGNORE;
    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_ARTIFACT_BASE_URI,
             description = "Location of worker jar and other artifacts needed (e.g., s3://benchmarking.neo4j.com/artifacts/<build_id>/) in S3",
             title = "Location of worker jar" )
    @Required
    private URI artifactBaseUri;

    private static JobScheduler getJobScheduler( InfraParams infraParams, String jobQueue, String jobDefinition, String batchStack )
    {
        if ( infraParams.hasAwsCredentials() )
        {
            return AWSBatchJobScheduler.create( infraParams.awsRegion(),
                                                infraParams.awsKey(),
                                                infraParams.awsSecret(),
                                                jobQueue,
                                                jobDefinition,
                                                batchStack );
        }
        else
        {
            return AWSBatchJobScheduler.create( infraParams.awsRegion(),
                                                jobQueue,
                                                jobDefinition,
                                                batchStack );
        }
    }

    private static AWSS3ArtifactStorage getAWSS3ArtifactStorage( InfraParams infraParams )
    {
        if ( infraParams.hasAwsCredentials() )
        {
            return AWSS3ArtifactStorage.create( infraParams.awsRegion(),
                                                infraParams.awsKey(),
                                                infraParams.awsSecret() );
        }
        else
        {
            return AWSS3ArtifactStorage.create( infraParams.awsRegion() );
        }
    }

    private List<JobStatus> jobStatuses( JobScheduler jobScheduler, InfraParams infraParams, JobId jobId )
    {
        List<JobStatus> jobStatuses =
                jobScheduler.jobsStatuses( Collections.singletonList( jobId ) );
        LOG.info( "current jobs statuses:\n{}",
                  jobStatuses.stream()
                             .map( status -> status.toStatusLine( infraParams.awsRegion() ) ).collect( joining( "\n" ) ) );
        return jobStatuses;
    }

    @Override
    protected final void doRun( RunWorkloadParams runWorkloadParams )
    {
        InfraParams infraParams = new InfraParams( awsSecret,
                                                   awsKey,
                                                   awsRegion,
                                                   storeName,
                                                   resultsStoreUsername,
                                                   resultsStorePasswordSecretName,
                                                   resultsStoreUri,
                                                   artifactBaseUri,
                                                   errorReportingPolicy );
        try
        {

            // first store job params as JSON
            JobParams jobParams = new JobParams( infraParams, runWorkloadParams );
            Path workspacePath = workspaceDir.toPath();
            Files.write( workspacePath.resolve( Workspace.JOB_PARAMETERS_JSON ), jobParams.toJson().getBytes( StandardCharsets.UTF_8 ) );

            Workspace workspace = Workspace.defaultMacroWorkspace( workspacePath,
                                                                   runWorkloadParams.neo4jVersion(),
                                                                   runWorkloadParams.neo4jEdition()
            );

            AWSS3ArtifactStorage artifactStorage = getAWSS3ArtifactStorage( infraParams );

            URI artifactBaseURI = infraParams.artifactBaseUri();
            artifactStorage.uploadBuildArtifacts( artifactBaseURI, workspace );
            LOG.info( "upload build artifacts into {}", artifactBaseURI );

            JobScheduler jobScheduler = getJobScheduler( infraParams, jobQueue, jobDefinition, batchStack );

            JobId jobId = jobScheduler.schedule( artifactWorkerUri, artifactBaseURI, runWorkloadParams );
            LOG.info( "job scheduled, with id {}", jobId.id() );

            // wait until they are done, or fail
            RetryPolicy<List<JobStatus>> retries = new RetryPolicy<List<JobStatus>>()
                    .handleResultIf( jobsStatuses -> jobsStatuses.stream().anyMatch( JobStatus::isWaiting ) )
                    .withDelay( Duration.ofMinutes( 5 ) )
                    .withMaxAttempts( -1 );

            List<JobStatus> jobsStatuses = Failsafe.with( retries ).get( () -> jobStatuses( jobScheduler, infraParams, jobId ) );
            LOG.info( "jobs are done with following statuses\n{}", jobsStatuses.stream().map( Object::toString ).collect( joining( "\n" ) ) );

            // if any of the jobs failed, fail whole run
            if ( jobsStatuses.stream().anyMatch( JobStatus::isFailed ) )
            {
                throw new RuntimeException( "there are failed jobs:\n" +
                                            jobsStatuses.stream()
                                                        .filter( JobStatus::isFailed )
                                                        .map( Object::toString )
                                                        .collect( joining( "\n" ) ) );
            }
        }
        catch ( ArtifactStoreException | IOException e )
        {
            throw new RuntimeException( "failed to schedule benchmarking job", e );
        }
    }
}
