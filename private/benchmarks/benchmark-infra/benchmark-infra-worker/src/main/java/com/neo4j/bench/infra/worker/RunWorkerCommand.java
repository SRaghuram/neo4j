/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.worker;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.infra.BenchmarkingTool;
import com.neo4j.bench.infra.BenchmarkingToolRunner;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.PasswordManager;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSPasswordManager;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;

@Command( name = "run-worker" )
public class RunWorkerCommand implements Runnable
{

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
            Path parameterFilePath = artifactStorage.downloadParameterFile( workspacePath, artifactBaseUri );
            JobParams jobParams = JsonUtil.deserializeJson( parameterFilePath, JobParams.class );
            InfraParams infraParams = jobParams.infraParams();

            Workspace deserializeWorkspace = infraParams.workspaceStructure();
            Workspace artifactsWorkspace = artifactStorage.downloadBuildArtifacts( workspacePath,
                                                                                   artifactBaseUri,
                                                                                   deserializeWorkspace );

            Workspace.assertWorkspaceAreEqual( artifactsWorkspace, deserializeWorkspace );

            // fetch result db password
            PasswordManager awsSecretsManager = AWSPasswordManager.create( infraParams.awsRegion() );
            String resultsStorePassword = awsSecretsManager.getSecret( infraParams.resultsStorePasswordSecretName() );

            BenchmarkingTool benchmarkingTool = jobParams.benchmarkingEnvironment().benchmarkingTool();
            BenchmarkingToolRunner toolRunner = benchmarkingTool.newRunner();
            toolRunner.runTool( benchmarkingTool.getToolParameters(),
                                artifactStorage,
                                workspacePath,
                                artifactsWorkspace,
                                jobParams.infraParams(),
                                resultsStorePassword,
                                batchJobId,
                                artifactBaseUri );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "fatal error in worker", e );
        }
    }
}
