/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.BaseRunWorkloadCommand;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.infra.InfraParams;

import java.net.URI;

import static com.neo4j.bench.common.tool.macro.RunWorkloadParams.CMD_ERROR_POLICY;

public abstract class BaseInfraCommand extends BaseRunWorkloadCommand
{

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
        doRunInfra( runWorkloadParams, infraParams );
    }

    protected abstract void doRunInfra( RunWorkloadParams runWorkloadParams, InfraParams infraParams );
}
