/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.env;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.ComputeEnvironmentDetail;
import com.amazonaws.services.batch.model.ComputeResource;
import com.amazonaws.services.batch.model.DescribeComputeEnvironmentsResult;
import com.neo4j.bench.model.model.Instance;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import oshi.SystemInfo;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;

public class InstanceDiscoveryTest
{

    @Test
    public void discoverServerInstance() throws Exception
    {
        // given
        InstanceDiscovery instanceDiscovery = new InstanceDiscovery( () ->
                                                                     {
                                                                         throw new RuntimeException( "shouldn't be called when discovering service host" );
                                                                     } );
        // when
        Instance instance = instanceDiscovery.currentInstance( System.getenv() );
        // then
        SystemInfo systemInfo = new SystemInfo();
        assertEquals( SystemUtils.OS_NAME + ", " + SystemUtils.OS_VERSION, instance.operatingSystem() );
        assertEquals( systemInfo.getHardware().getProcessor().getLogicalProcessorCount(), instance.availableProcessors() );
        assertEquals( systemInfo.getHardware().getMemory().getTotal(), instance.totalMemory() );
        assertEquals( InetAddress.getLocalHost().getHostName(), instance.host() );
        assertEquals( Instance.Kind.Server, instance.kind() );
    }

    @Test
    public void discoverComputeEnvironmentInstance() throws Exception
    {
        // given
        AWSBatch awsBatch = mock( AWSBatch.class );
        Mockito.when( awsBatch.describeComputeEnvironments( argThat( arg -> arg.getComputeEnvironments().contains( "compute-enviroment-name" ) ) ) )
               .thenReturn( new DescribeComputeEnvironmentsResult()
                                    .withComputeEnvironments(
                                            new ComputeEnvironmentDetail().withComputeResources(
                                                    new ComputeResource().withInstanceTypes( "instanceType" ) ) ) );

        Map<String,String> sysenv = new HashMap<>( System.getenv() );
        sysenv.put( "AWS_BATCH_CE_NAME", "compute-enviroment-name" );

        InstanceDiscovery instanceDiscovery = new InstanceDiscovery( () -> awsBatch );
        // when
        Instance instance = instanceDiscovery.currentInstance( sysenv );
        // then
        SystemInfo systemInfo = new SystemInfo();
        assertEquals( SystemUtils.OS_NAME + ", " + SystemUtils.OS_VERSION, instance.operatingSystem() );
        assertEquals( systemInfo.getHardware().getProcessor().getLogicalProcessorCount(), instance.availableProcessors() );
        assertEquals( systemInfo.getHardware().getMemory().getTotal(), instance.totalMemory() );
        assertEquals( "instanceType", instance.host() );
        assertEquals( Instance.Kind.AWS, instance.kind() );
    }

    @Test
    public void errorWhenNoComputeEnvironmentsFound()
    {
        // given
        AWSBatch awsBatch = mock( AWSBatch.class );
        Mockito.when( awsBatch.describeComputeEnvironments( argThat( arg -> arg.getComputeEnvironments().contains( "compute-enviroment-name" ) ) ) )
               .thenReturn( new DescribeComputeEnvironmentsResult().withComputeEnvironments( emptyList() ) );

        Map<String,String> sysenv = new HashMap<>( System.getenv() );
        sysenv.put( "AWS_BATCH_CE_NAME", "compute-enviroment-name" );

        InstanceDiscovery instanceDiscovery = new InstanceDiscovery( () -> awsBatch );
        // when
        assertThrows( RuntimeException.class, () -> instanceDiscovery.currentInstance( sysenv ), "unexpected empty instance types" );
    }
}
