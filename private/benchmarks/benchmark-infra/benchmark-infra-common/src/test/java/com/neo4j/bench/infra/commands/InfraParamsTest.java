/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.util.JsonUtil;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.InfraParams;
import org.junit.jupiter.api.Test;
import com.neo4j.bench.infra.Workspace;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfraParamsTest
{

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void serializationTest() throws IOException
    {
        InfraParams infraParams = new InfraParams( new AWSCredentials( "awsSecret",
                                                                       "awsKey",
                                                                       "awsRegion" ),
                                                   "resultStoreUsername",
                                                   "resultStorePasswordSecretName",
                                                   URI.create( "http://resultStoreUri" ),
                                                   URI.create( "http://artifactBaseUri" ),
                                                   ErrorReportingPolicy.FAIL,
                                                   Workspace.create( temporaryFolder.newFolder().toPath() ).build() );
        InfraParams actualInfraParams = JsonUtil.deserializeJson( JsonUtil.serializeJson( infraParams ), InfraParams.class );
        assertEquals( infraParams, actualInfraParams );
    }
}
