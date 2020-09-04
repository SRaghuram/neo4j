/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfraParamsTest
{

    @Test
    public void serializationTest( @TempDir Path tempDir ) throws IOException
    {
        InfraParams infraParams = new InfraParams( new AWSCredentials( "awsKey",
                                                                       "awsSecret",
                                                                       "awsRegion" ),
                                                   "resultStoreUsername",
                                                   "resultStorePasswordSecretName",
                                                   URI.create( "http://resultStoreUri" ),
                                                   URI.create( "http://artifactBaseUri" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( tempDir ).build() );
        InfraParams actualInfraParams = JsonUtil.deserializeJson( JsonUtil.serializeJson( infraParams ), InfraParams.class );
        assertEquals( infraParams, actualInfraParams );
    }
}
