/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.commands;

import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.util.JsonUtil;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class InfraParamsTest
{

    @Test
    public void serializationTest()
    {
        InfraParams infraParams = new InfraParams( "awsSecret",
                                                   "awsKey",
                                                   "awsRegion",
                                                   "storeName",
                                                   "resultStoreUsername",
                                                   "resultStorePasswordSecretName",
                                                   URI.create( "http://resultStoreUri" ),
                                                   URI.create( "http://artifactBaseUri" ),
                                                   ErrorReportingPolicy.FAIL );
        InfraParams actualInfraParams = JsonUtil.deserializeJson( JsonUtil.serializeJson( infraParams ), InfraParams.class );
        assertEquals( infraParams, actualInfraParams );
    }
}
