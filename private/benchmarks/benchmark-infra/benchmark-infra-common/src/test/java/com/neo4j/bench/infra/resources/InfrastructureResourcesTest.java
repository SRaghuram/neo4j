/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.ComputeEnvironmentDetail;
import com.amazonaws.services.batch.model.ComputeEnvironmentOrder;
import com.amazonaws.services.batch.model.ComputeResource;
import com.amazonaws.services.batch.model.ContainerProperties;
import com.amazonaws.services.batch.model.DescribeComputeEnvironmentsResult;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsResult;
import com.amazonaws.services.batch.model.DescribeJobQueuesResult;
import com.amazonaws.services.batch.model.JobDefinition;
import com.amazonaws.services.batch.model.JobQueueDetail;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.model.DescribeStackResourcesResult;
import com.amazonaws.services.cloudformation.model.StackResource;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesResult;
import com.amazonaws.services.ec2.model.InstanceTypeInfo;
import com.amazonaws.services.ec2.model.MemoryInfo;
import com.amazonaws.services.ec2.model.VCpuInfo;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

public class InfrastructureResourcesTest
{

    @Mock
    private AmazonCloudFormation amazonCloudFormation;
    @Mock
    private AWSBatch awsBatch;
    @Mock
    private AmazonEC2 amazonEC2;

    @BeforeEach
    public void setUp()
    {
        MockitoAnnotations.initMocks( this );
    }

    @Test
    public void findJobDefinition()
    {

        // given
        when( amazonCloudFormation.describeStackResources( argThat( arg -> "stackName".equals( arg.getStackName() ) ) ) )
                .thenReturn( new DescribeStackResourcesResult().withStackResources(
                        new StackResource().withResourceType( "AWS::Batch::JobDefinition" )
                                           .withPhysicalResourceId( "arn:aws:batch:eu-north-1:535893049302:job-queue/R5D2XLargeJobQueue-8153279" )
                ) );

        when( awsBatch.describeJobDefinitions(
                argThat( arg -> arg.getJobDefinitions().contains( "arn:aws:batch:eu-north-1:535893049302:job-queue/R5D2XLargeJobQueue-8153279" ) ) ) )
                .thenReturn( new DescribeJobDefinitionsResult().withJobDefinitions(
                        new JobDefinition()
                                .withJobDefinitionName( "jobDefinitionName" )
                                .withContainerProperties(
                                        new ContainerProperties().withMemory( 31000 )
                                                                 .withVcpus( 8 )
                                                                 .withImage( "535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:oracle_11" )
                                )
                ) );

        InfrastructureResources infrastructureResources = new InfrastructureResources( amazonCloudFormation, awsBatch, amazonEC2 );

        // when
        Collection<InfrastructureResource> resources = infrastructureResources.findResources( "stackName" );

        // then
        JobDefinitionResource jobDefinitionResource =
                resources.stream()
                         .filter( JobDefinitionResource.class::isInstance )
                         .map( JobDefinitionResource.class::cast )
                         .findFirst()
                         .get();
        assertEquals( "jobDefinitionName", jobDefinitionResource.jobDefinitionName() );
        assertEquals( ImmutableSet.of(
                Hardware.availableCores( 8 ),
                Hardware.totalMemory( 31000 ),
                Jdk.of( "oracle", "11" )
        ), jobDefinitionResource.capabilities() );
    }

    @Test
    public void findComputeEnvironment()
    {
        // given
        String computeEnvironmentArn = "arn:aws:batch:eu-north-1:535893049302:compute-environment/M5D2XLargeComputeEnviro-663db725e2ddd95";
        String jobQueueArn = "arn:aws:batch:eu-north-1:535893049302:job-queue/R5D2XLargeJobQueue-8153279";
        when( amazonCloudFormation.describeStackResources( argThat( arg -> "stackName".equals( arg.getStackName() ) ) ) )
                .thenReturn( new DescribeStackResourcesResult().withStackResources(
                        new StackResource().withResourceType( "AWS::Batch::JobQueue" )
                                           .withPhysicalResourceId( jobQueueArn ),
                        new StackResource().withResourceType( "AWS::Batch::ComputeEnvironmentResource" )
                                           .withPhysicalResourceId(
                                                   computeEnvironmentArn )
                ) );

        when( awsBatch.describeJobQueues( argThat(
                arg -> arg.getJobQueues()
                          .contains( jobQueueArn ) ) ) )
                .thenReturn( new DescribeJobQueuesResult().withJobQueues(
                        new JobQueueDetail().withJobQueueName( "jobQueueName" )
                                            .withComputeEnvironmentOrder( new ComputeEnvironmentOrder().withComputeEnvironment( computeEnvironmentArn ) )
                ) );
        when( awsBatch.describeComputeEnvironments( argThat( arg -> arg.getComputeEnvironments().contains( computeEnvironmentArn ) ) ) )
                .thenReturn( new DescribeComputeEnvironmentsResult().withComputeEnvironments(
                        new ComputeEnvironmentDetail().withComputeResources(
                                new ComputeResource().withInstanceTypes( "r5d.2xlarge" )
                        )
                ) );

        when( amazonEC2.describeInstanceTypes( argThat( arg -> arg.getInstanceTypes().contains( "r5d.2xlarge" ) ) ) )
                .thenReturn( new DescribeInstanceTypesResult().withInstanceTypes(
                        new InstanceTypeInfo()
                                .withMemoryInfo( new MemoryInfo().withSizeInMiB( 3100L ) )
                                .withVCpuInfo( new VCpuInfo().withDefaultVCpus( 8 ) )
                                .withInstanceType( "r5d.2xlarge" )
                ) );

        InfrastructureResources infrastructureResources = new InfrastructureResources( amazonCloudFormation, awsBatch, amazonEC2 );

        // when
        Collection<InfrastructureResource> resources = infrastructureResources.findResources( "stackName" );

        // then
        ComputeEnvironmentResource computerEnvironmentResource =
                resources.stream()
                         .filter( ComputeEnvironmentResource.class::isInstance )
                         .map( ComputeEnvironmentResource.class::cast )
                         .findFirst()
                         .get();
        assertEquals( "jobQueueName", computerEnvironmentResource.queueName() );
        assertEquals( ImmutableSet.of( Hardware.availableCores( 8 ),
                                       Hardware.totalMemory( 3100 ),
                                       AWS.instanceType( "r5d.2xlarge" ) ),
                      computerEnvironmentResource.capabilities() );
    }
}
