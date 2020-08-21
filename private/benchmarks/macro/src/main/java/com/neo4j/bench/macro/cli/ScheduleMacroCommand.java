/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.DeploymentModes;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingEnvironment;
import com.neo4j.bench.infra.BenchmarkingTool;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.URIHelper;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.macro.MacroToolRunner;
import com.neo4j.bench.infra.scheduler.BenchmarkJobScheduler;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.util.JsonUtil;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_ERROR_POLICY;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Command( name = "schedule" )
public class ScheduleMacroCommand extends BaseRunWorkloadCommand
{

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
             name = {RunMacroWorkloadParams.CMD_DB_NAME},
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
             description = "Location of worker jar and other artifacts needed " +
                           "(e.g., s3://benchmarking.neo4j.com/artifacts/macro/<triggered_by>/<build_id>/<workload>/<query>/<uuid>) in S3",
             title = "Location of worker jar" )
    @Required
    private URI artifactBaseUri;

    private static String getJobName( String tool, String benchmark, String version, String triggered )
    {
        return format( "%s-%s-%s-%s", tool, benchmark, version, triggered );
    }

    @Override
    protected final void doRun( RunMacroWorkloadParams runMacroWorkloadParams )
    {
        try
        {
            // first start preparing the workspace
            Path workspacePath = workspaceDir.toPath();

            File jobParameterJson = workspacePath.resolve( Workspace.JOB_PARAMETERS_JSON ).toFile();
            jobParameterJson.createNewFile();

            Workspace workspace = null;
            if ( runMacroWorkloadParams.deployment().deploymentModes().equals( DeploymentModes.SERVER ) )
            {
                Deployment.Server server = (Deployment.Server) runMacroWorkloadParams.deployment();
                workspace = Workspace.defaultMacroServerWorkspace( workspacePath, server.path().toString() );
            }
            else
            {
                workspace = Workspace.defaultMacroEmbeddedWorkspace( workspacePath );
            }

            AWSCredentials awsCredentials = new AWSCredentials( awsKey, awsSecret, awsRegion );
            BenchmarkJobScheduler benchmarkJobScheduler = BenchmarkJobScheduler.create( jobQueue, jobDefinition, batchStack, awsCredentials );
            URI artifactBaseWorkloadURI = artifactBaseUri.resolve( URIHelper.toURIPart( runMacroWorkloadParams.triggeredBy() ) )
                                                         .resolve( URIHelper.toURIPart( runMacroWorkloadParams.teamcityBuild().toString() ) )
                                                         .resolve( URIHelper.toURIPart( runMacroWorkloadParams.workloadName() ) );

            try ( Resources resources = new Resources( Paths.get( "." ) ) )
            {
                Workload workload = Workload.fromName( runMacroWorkloadParams.workloadName(), resources, runMacroWorkloadParams.deployment() );
                List<List<Query>> partitions = Lists.partition( workload.queries(), workload.queryPartitionSize() );

                for ( List<Query> queries : partitions )
                {

                    List<String> queryNames = queries.stream().map( Query::name ).collect( toList() );
                    UUID uuid = UUID.randomUUID();
                    URI artifactBaseQueryRunURI = artifactBaseWorkloadURI.resolve( URIHelper.toURIPart( uuid.toString() ) );
                    URI artifactWorkerQueryRunURI = artifactBaseQueryRunURI.resolve( "benchmark-infra-worker.jar" );
                    // then store job params as JSON
                    InfraParams infraParams = new InfraParams( awsCredentials,
                                                               resultsStoreUsername,
                                                               resultsStorePasswordSecretName,
                                                               resultsStoreUri,
                                                               artifactBaseQueryRunURI,
                                                               errorReportingPolicy,
                                                               workspace );
                    JobParams jobParams = new JobParams( infraParams,
                                                         new BenchmarkingEnvironment(
                                                                 new BenchmarkingTool( MacroToolRunner.class,
                                                                                       new RunToolMacroWorkloadParams(
                                                                                               runMacroWorkloadParams.setQueryNames( queryNames ),
                                                                                               storeName ) ) ) );

                    workspace.assertArtifactsExist();

                    benchmarkJobScheduler.scheduleBenchmarkJob( getJobName( "macro",
                                                                            workload.name(),
                                                                            runMacroWorkloadParams.neo4jVersion().toString(),
                                                                            runMacroWorkloadParams.triggeredBy() ),
                                                                jobParams,
                                                                workspace,
                                                                artifactWorkerQueryRunURI,
                                                                jobParameterJson );
                }
                benchmarkJobScheduler.awaitFinished();
            }
        }
        catch ( ArtifactStoreException | IOException e )
        {
            throw new RuntimeException( "failed to schedule benchmarking job", e );
        }
    }
}
