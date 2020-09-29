/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.env;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.ComputeEnvironmentDetail;
import com.amazonaws.services.batch.model.ComputeResource;
import com.amazonaws.services.batch.model.DescribeComputeEnvironmentsRequest;
import com.neo4j.bench.model.model.Instance;
import org.apache.commons.lang3.SystemUtils;
import oshi.SystemInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.String.format;

public class InstanceDiscovery
{

    private final Supplier<AWSBatch> awsBatch;

    public static InstanceDiscovery create()
    {
        return new InstanceDiscovery( AWSBatchClientBuilder::defaultClient );
    }

    InstanceDiscovery( Supplier<AWSBatch> awsBatch )
    {
        this.awsBatch = awsBatch;
    }

    public Instance currentInstance( Map<String,String> env )
    {
        SystemInfo systemInfo = new SystemInfo();
        if ( env.containsKey( "AWS_BATCH_CE_NAME" ) )
        {
            String computeEnvName = env.get( "AWS_BATCH_CE_NAME" );
            String[] instanceTypes = getInstanceTypes( computeEnvName );

            if ( instanceTypes.length != 1 )
            {
                throw new RuntimeException(
                        format( "unexpected %s instance types", instanceTypes.length == 0 ? "empty" : Arrays.toString( instanceTypes ) ) );
            }
            return Instance.aws( instanceTypes[0],
                                 currentOperatingSystem(),
                                 systemInfo.getHardware().getProcessor().getLogicalProcessorCount(),
                                 systemInfo.getHardware().getMemory().getTotal() );
        }
        else
        {
            return Instance.server( currentServer(),
                                    currentOperatingSystem(),
                                    systemInfo.getHardware().getProcessor().getLogicalProcessorCount(),
                                    systemInfo.getHardware().getMemory().getTotal() );
        }
    }

    private String[] getInstanceTypes( String computeEnvName )
    {
        return awsBatch.get()
                       .describeComputeEnvironments( new DescribeComputeEnvironmentsRequest().withComputeEnvironments( computeEnvName ) )
                       .getComputeEnvironments()
                       .stream()
                       .findFirst()
                       .map( ComputeEnvironmentDetail::getComputeResources )
                       .map( ComputeResource::getInstanceTypes )
                       .orElseGet( Collections::emptyList )
                       .toArray( new String[]{} );
    }

    private static String currentOperatingSystem()
    {
        return SystemUtils.OS_NAME + ", " + SystemUtils.OS_VERSION;
    }

    private static String currentServer()
    {
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }
}
