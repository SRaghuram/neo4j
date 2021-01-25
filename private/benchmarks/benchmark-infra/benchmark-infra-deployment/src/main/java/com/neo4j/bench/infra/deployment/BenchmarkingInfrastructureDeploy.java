/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.deployment;

import com.neo4j.bench.model.util.JsonUtil;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Environment;
import software.amazon.awscdk.core.StackProps;

import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;

public class BenchmarkingInfrastructureDeploy
{

    public static void main( final String[] args )
    {

        Environment env = Environment.builder()
                                     .account( System.getenv( "CDK_DEFAULT_ACCOUNT" ) )
                                     .region( System.getenv( "CDK_DEFAULT_REGION" ) )
                                     .build();

        App app = new App();
        String batchStack = requireNonNull( (String) app.getNode().tryGetContext( "batch-stack" ),
                                            "batch-stack context value is required" );
        String batchInfrastructureDefFile = requireNonNull( (String) app.getNode().tryGetContext( "batch-infrastructure" ),
                                                            "batch-infrastructure context value is required" );

        BatchInfrastructure batchInfrastructure = JsonUtil.deserializeJson( Paths.get( batchInfrastructureDefFile ), BatchInfrastructure.class );

        new BatchStack( app,
                        batchStack,
                        StackProps.builder().env( env ).build(),
                        batchInfrastructure );

        app.synth();
    }
}
