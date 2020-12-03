/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.deployment;

import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchInfrastructureTest
{

    @Test
    public void serializeJson()
    {
        HashSet<String> jdks = new HashSet<>();
        jdks.add( "oracle-11" );
        jdks.add( "oracle-8" );

        Map<String,BatchEnvironment> batchEnvironments = new HashMap<>();
        batchEnvironments.put( "small",
                               new BatchEnvironment(
                                       InstanceType.A1_2_XLARGE,
                                       jdks,
                                       15 ) );
        BatchInfrastructure batchInfrastructure = new BatchInfrastructure(
                batchEnvironments,
                "vpc",
                "defaultSecurityGroup",
                Collections.singletonList( "resultStorePassword" ) );
        String json = JsonUtil.serializeJson( batchInfrastructure );
        BatchInfrastructure deserialized = JsonUtil.deserializeJson( json, BatchInfrastructure.class );
        assertEquals( batchInfrastructure, deserialized );
    }
}
