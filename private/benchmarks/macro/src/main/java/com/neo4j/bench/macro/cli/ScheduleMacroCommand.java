/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.common.command.ResultsStoreArgs;
import com.neo4j.bench.common.database.AllSupportedVersions;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.DeploymentModes;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.common.tool.macro.RunToolMacroWorkloadParams;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkingRun;
import com.neo4j.bench.infra.BenchmarkingTool;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.PasswordManager;
import com.neo4j.bench.infra.ResultStoreCredentials;
import com.neo4j.bench.infra.URIHelper;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.macro.MacroToolRunner;
import com.neo4j.bench.infra.resources.Infrastructure;
import com.neo4j.bench.infra.resources.InfrastructureCapabilities;
import com.neo4j.bench.infra.resources.InfrastructureMatcher;
import com.neo4j.bench.infra.resources.InfrastructureResources;
import com.neo4j.bench.infra.scheduler.BenchmarkJobScheduler;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_ERROR_POLICY;
import static com.neo4j.bench.infra.InfraNamesHelper.sanitizeOutputLogicalId;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.removeEnd;

@Command( name = "schedule" )
public class ScheduleMacroCommand extends BaseRunWorkloadCommand
{

    private static Logger LOG = LoggerFactory.getLogger( ScheduleMacroCommand.class );
    private static final String DEFAULT_UPGRADE_JOB_DEFINITION_NAME = "MacroUpgradeJobDefinition-openjdk-11";

    // job queue name in CloudFormation stack
    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_JOB_QUEUE,
            title = "AWS Batch Job Queue Name",
            description = "job queue name in CloudFormation stack" )
    private String jobQueue;

    // job definition in CloudFormation stack
    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_JOB_DEFINITION,
            title = "AWS Batch Job Definition Name",
            description = "job definition in CloudFormation stack" )
    private String jobDefinition;

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
            name = {InfraParams.CMD_RESULTS_STORE_PASSWORD_SECRET_NAME},
            description = "Secret name in AWS Secrets Manager with password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password Secret Name" )
    private String resultsStorePasswordSecretName;

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

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
    private URI artifactBaseUri = URI.create( "s3://storage.benchmarking.neo4j.today/artifacts/macro/" );

    private static final String CMD_INFRASTRUCTURE_CAPABILITIES = "--infrastructure-capabilities";
    @Option( type = OptionType.COMMAND,
            name = CMD_INFRASTRUCTURE_CAPABILITIES,
            description = "set of required infrastructure capabilities to run benchmark " +
                          "(e.g., hardware:totalMemory=16384,hardware:availableCores=8,jdk=oracle-8 " +
                          "or AWS:instanceType=m5d.xlarge,jdk=oracle-8)",
            title = "Infrastructure capabilities" )
    private String infrastructureCapabilities;

    private static final String CMD_DATASET_BASE_URI = "--dataset-base-uri";
    @Option( type = OptionType.COMMAND,
            name = CMD_DATASET_BASE_URI,
            description = "S3 base uri to location with macro datasets",
            title = "Dataset base S3 URI" )
    private URI dataSetBaseUri = URI.create( "s3://storage.benchmarking.neo4j.today/datasets/macro/" );

    private static final String CMD_RECORDINGS_BASE_URI = "--recordings-base-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RECORDINGS_BASE_URI},
            description = "S3 bucket recordings and profiles were uploaded to",
            title = "Recordings and profiles S3 URI" )
    private URI recordingsBaseUri = URI.create( "s3://storage.benchmarking.neo4j.today/recordings/" );

    private static final String CMD_UPGRADE_STORE = "--upgrade-store";
    @Option( type = OptionType.COMMAND,
            name = {CMD_UPGRADE_STORE},
            description = "If set upgrade store before running and use this store",
            title = "Upgrade store" )
    private boolean upgradeStore;

    private static String getJobName( String tool, String benchmark, String version, String triggered )
    {
        return format( "%s-%s-%s-%s", tool, benchmark, version, triggered );
    }

    @Override
    protected final void doRun( RunMacroWorkloadParams runMacroWorkloadParams )
    {
        AWSCredentials awsCredentials = new AWSCredentials( awsKey, awsSecret, awsRegion );
        try ( ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( awsCredentials );
              Resources resources = new Resources( Paths.get( "." ) ) )
        {
            Infrastructure infrastructure = matchInfrastructure( awsCredentials );

            // first start preparing the workspace
            Path workspacePath = workspaceDir.toPath();

            Workspace workspace = prepareWorkspace( runMacroWorkloadParams, workspacePath );

            BenchmarkJobScheduler benchmarkJobScheduler = BenchmarkJobScheduler.create( infrastructure, batchStack, awsCredentials, artifactStorage );
            URI artifactBaseWorkloadURI = artifactBaseUri.resolve( URIHelper.toURIPart( runMacroWorkloadParams.triggeredBy() ) )
                                                         .resolve( URIHelper.toURIPart( runMacroWorkloadParams.teamcityBuild().toString() ) )
                                                         .resolve( URIHelper.toURIPart( runMacroWorkloadParams.workloadName() ) );

            InfraParams infraParams = new InfraParams( awsCredentials,
                                                       resultsStoreArgs.resultsStoreUsername(),
                                                       resultsStorePasswordSecretName,
                                                       resultsStoreArgs.resultsStorePassword(),
                                                       resultsStoreArgs.resultsStoreUri(),
                                                       null,
                                                       errorReportingPolicy,
                                                       workspace,
                                                       recordingsBaseUri );

            Workload workload = Workload.fromName( runMacroWorkloadParams.workloadName(), resources, runMacroWorkloadParams.deployment() );

            URI currentDataSetBaseUri = upgradeStoreIfNeeded( runMacroWorkloadParams,
                                                              awsCredentials,
                                                              artifactStorage,
                                                              workspace,
                                                              infraParams,
                                                              workload,
                                                              artifactBaseWorkloadURI );

            CompletableFuture<Void> awaitFinished = CompletableFuture.runAsync( benchmarkJobScheduler::awaitFinished );
            try
            {
                List<List<Query>> partitions = Lists.partition( workload.queries(), workload.queryPartitionSize() );

                for ( List<Query> queries : partitions )
                {
                    List<String> queryNames = queries.stream().map( Query::name ).collect( toList() );
                    UUID uuid = UUID.randomUUID();
                    URI artifactBaseQueryRunURI = artifactBaseWorkloadURI.resolve( URIHelper.toURIPart( uuid.toString() ) );
                    URI artifactWorkerQueryRunURI = artifactBaseQueryRunURI.resolve( "benchmark-infra-worker.jar" );
                    infraParams = infraParams.withArtifactBaseUri( artifactBaseQueryRunURI );

                    String testRunId = UUID.randomUUID().toString();

                    Deployment deployment = addArtifactBaseUriToProduct( runMacroWorkloadParams, artifactBaseQueryRunURI, workspace );
                    RunMacroWorkloadParams workloadParams = runMacroWorkloadParams.withQueryNames( queryNames )
                                                                                  .withDeployment( deployment );
                    JobParams<RunToolMacroWorkloadParams> jobParams = createJobParams( infraParams, testRunId, workloadParams, currentDataSetBaseUri );

                    workspace.assertArtifactsExist();

                    benchmarkJobScheduler.scheduleBenchmarkJob( getJobName( "macro",
                                                                            workload.name(),
                                                                            workloadParams.neo4jVersion().toString(),
                                                                            workloadParams.triggeredBy() ),
                                                                jobParams,
                                                                workspace,
                                                                artifactWorkerQueryRunURI );
                }
                awaitFinished.get();
            }
            finally
            {
                ResultStoreCredentials resultStoreCredentials = PasswordManager.getResultStoreCredentials( infraParams );
                try ( StoreClient storeClient = StoreClient.connect( resultStoreCredentials.uri(),
                                                                     resultStoreCredentials.username(),
                                                                     resultStoreCredentials.password() ) )
                {
                    benchmarkJobScheduler.reportJobsTo( storeClient );
                }
            }
        }
        catch ( ArtifactStoreException | IOException | InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( "failed to schedule benchmarking job", e );
        }
    }

    private URI upgradeStoreIfNeeded( RunMacroWorkloadParams runMacroWorkloadParams,
                                      AWSCredentials awsCredentials,
                                      ArtifactStorage artifactStorage,
                                      Workspace workspace,
                                      InfraParams infraParams,
                                      Workload workload,
                                      URI artifactBaseWorkloadURI ) throws ArtifactStoreException
    {
        if ( upgradeStore )
        {
            URI originDataSetBaseUri = URI.create( removeEnd( dataSetBaseUri.toString(), "/" ) );
            URI destDataSetBaseUri = artifactBaseWorkloadURI.resolve( "datasets" );
            LOG.info( "will try to upgrade store from{} into {}", originDataSetBaseUri, destDataSetBaseUri );

            String jobDefinitionName =
                    BenchmarkJobScheduler
                            .resolveOutputRef( sanitizeOutputLogicalId( DEFAULT_UPGRADE_JOB_DEFINITION_NAME ),
                                               awsCredentials.awsCredentialsProvider(),
                                               awsRegion,
                                               batchStack );
            LOG.info( "found job definition {}", jobDefinitionName );
            BenchmarkJobScheduler benchmarkJobScheduler = BenchmarkJobScheduler.create( "MacroStoreUpgradeJobQueue",
                                                                                        jobDefinitionName,
                                                                                        batchStack,
                                                                                        awsCredentials,
                                                                                        artifactStorage );

            String newVersion = runMacroWorkloadParams.neo4jVersion().minorVersion();
            benchmarkJobScheduler.scheduleStoreUpgrade( Workspace.defaultUpgradeStoreWorkspace( workspace ),
                                                        originDataSetBaseUri,
                                                        destDataSetBaseUri,
                                                        newVersion,
                                                        AllSupportedVersions.prevVersion( newVersion ),
                                                        workload.name(),
                                                        storeName,
                                                        infraParams.withArtifactBaseUri( artifactBaseWorkloadURI.resolve( "upgrade" ) ),
                                                        workload.recordFormat() );

            // await finished throws exception when there is job failure
            benchmarkJobScheduler.awaitFinished();

            return destDataSetBaseUri;
        }
        else
        {
            return dataSetBaseUri;
        }
    }

    private Workspace prepareWorkspace( RunMacroWorkloadParams runMacroWorkloadParams, Path workspacePath ) throws IOException
    {
        File jobParameterJson = workspacePath.resolve( Workspace.JOB_PARAMETERS_JSON ).toFile();
        jobParameterJson.createNewFile();

        if ( runMacroWorkloadParams.deployment().deploymentModes().equals( DeploymentModes.SERVER ) )
        {
            Deployment.Server server = (Deployment.Server) runMacroWorkloadParams.deployment();
            return Workspace.defaultMacroServerWorkspace( workspacePath, server.path().toString() );
        }
        else
        {
            return Workspace.defaultMacroEmbeddedWorkspace( workspacePath );
        }
    }

    private Infrastructure matchInfrastructure( AWSCredentials awsCredentials )
    {
        Infrastructure infrastructure;
        if ( isNotEmpty( jobDefinition ) && isNotEmpty( jobQueue ) && isEmpty( infrastructureCapabilities ) )
        {
            infrastructure = new Infrastructure(
                    BenchmarkJobScheduler.resolveOutputRef( jobQueue, awsCredentials.awsCredentialsProvider(), awsCredentials.awsRegion(), batchStack ),
                    jobDefinition );
        }
        else if ( isEmpty( jobDefinition ) && isEmpty( jobQueue ) && isNotEmpty( infrastructureCapabilities ) )
        {
            InfrastructureCapabilities capabilities = InfrastructureCapabilities.fromArgs( infrastructureCapabilities );
            InfrastructureMatcher infrastructureMatcher = new InfrastructureMatcher( InfrastructureResources.create( awsCredentials ) );
            infrastructure = infrastructureMatcher.findInfrastructure( batchStack, capabilities );
        }
        else
        {
            throw new IllegalArgumentException( format( "either provide %s or (%s and %s) command line arguments",
                                                        CMD_INFRASTRUCTURE_CAPABILITIES,
                                                        InfraParams.CMD_JOB_DEFINITION,
                                                        InfraParams.CMD_JOB_QUEUE ) );
        }
        return infrastructure;
    }

    private JobParams<RunToolMacroWorkloadParams> createJobParams( InfraParams infraParams, String testRunId, RunMacroWorkloadParams workloadParams,
                                                                   URI dataSetBaseUri )
    {
        URI datasetUri = AWSS3ArtifactStorage.createDatasetUri( dataSetBaseUri, workloadParams.neo4jVersion(), storeName );
        RunToolMacroWorkloadParams toolParams = new RunToolMacroWorkloadParams( workloadParams, datasetUri );
        return new JobParams<>( infraParams, new BenchmarkingRun<>( new BenchmarkingTool<>( MacroToolRunner.class, toolParams ), testRunId ) );
    }

    private Deployment addArtifactBaseUriToProduct( RunMacroWorkloadParams runMacroWorkloadParams, URI artifactBaseUri, Workspace workspace )
    {
        if ( runMacroWorkloadParams.deployment().deploymentModes().equals( DeploymentModes.SERVER ) )
        {
            URI s3URI = artifactBaseUri.resolve( workspace.getString( Workspace.NEO4J_ARCHIVE ) );
            return Deployment.server( s3URI.toString() );
        }
        else
        {
            return runMacroWorkloadParams.deployment();
        }
    }
}
