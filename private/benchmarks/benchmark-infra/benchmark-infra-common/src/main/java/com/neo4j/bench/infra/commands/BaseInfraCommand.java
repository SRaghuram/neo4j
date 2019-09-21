/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.tool.macro.BaseRunWorkloadCommand;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;

public abstract class BaseInfraCommand extends BaseRunWorkloadCommand
{

    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_USER},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_PASSWORD},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    @Option( type = OptionType.COMMAND,
             name = {InfraParams.CMD_RESULTS_STORE_URI},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_WORKSPACE_DIR,
             description = "Local directory containing artifacts to be uploaded to S3, which the worker requires",
             title = "Local workspace" )
    @Required
    private File workspaceDir;

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
             name = InfraParams.CMD_ARTIFACT_BASE_URI,
             description = "Location of worker jar and other artifacts needed (e.g., s3://benchmarking.neo4j.com/artifacts/<build_id>/) in S3",
             title = "Location of worker jar" )
    @Required
    private URI artifactBaseUri;

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_ARTIFACT_WORKER_URI,
             description = "Location of worker jar(e.g., s3://benchmarking.neo4j.com/artifacts/<build_id>/) in S3",
             title = "Location of worker jar" )
    @Required
    private URI artifactWorkerUri;

    @Override
    protected final void doRun( RunWorkloadParams runWorkloadParams )
    {
        InfraParams infraParams = new InfraParams( workspaceDir.toPath(),
                                                   awsSecret,
                                                   awsKey,
                                                   awsRegion,
                                                   storeName,
                                                   resultsStoreUsername,
                                                   resultsStorePassword,
                                                   resultsStoreUri,
                                                   artifactBaseUri,
                                                   artifactWorkerUri );
        doRunInfra( runWorkloadParams, infraParams );
    }

    protected abstract void doRunInfra( RunWorkloadParams runWorkloadParams, InfraParams infraParams );
}
