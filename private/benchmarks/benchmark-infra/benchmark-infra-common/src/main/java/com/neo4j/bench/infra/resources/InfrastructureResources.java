/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.AWSBatchException;
import com.amazonaws.services.batch.model.ComputeEnvironmentDetail;
import com.amazonaws.services.batch.model.ComputeEnvironmentOrder;
import com.amazonaws.services.batch.model.DescribeComputeEnvironmentsRequest;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsRequest;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsResult;
import com.amazonaws.services.batch.model.DescribeJobQueuesRequest;
import com.amazonaws.services.batch.model.JobDefinition;
import com.amazonaws.services.batch.model.JobQueueDetail;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.DescribeStackResourcesRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackResourcesResult;
import com.amazonaws.services.cloudformation.model.StackResource;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstanceTypesRequest;
import com.amazonaws.services.ec2.model.InstanceTypeInfo;
import com.neo4j.bench.infra.AWSCredentials;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class InfrastructureResources
{
    private static final Pattern JDK_VERSION_PATTERN = Pattern.compile( ":(.+?)_(\\d+)(_.+)?" );

    private final AmazonCloudFormation amazonCloudFormation;
    private final AWSBatch awsBatch;
    private final AmazonEC2 amazonEC2;

    InfrastructureResources( AmazonCloudFormation amazonCloudFormation, AWSBatch awsBatch, AmazonEC2 amazonEC2 )
    {

        this.amazonCloudFormation = amazonCloudFormation;
        this.awsBatch = awsBatch;
        this.amazonEC2 = amazonEC2;
    }

    public static InfrastructureResources create( AWSCredentials awsCredentials )
    {
        return new InfrastructureResources( amazonCloudFormation( awsCredentials ), awsBatch( awsCredentials ), amazonEC2( awsCredentials ) );
    }

    private static AmazonEC2 amazonEC2( AWSCredentials awsCredentials )
    {
        return awsCredentials.hasAwsCredentials() ? AmazonEC2ClientBuilder.standard()
                                                                          .withCredentials( awsCredentialsProvider( awsCredentials ) )
                                                                          .withRegion( awsCredentials.awsRegion() ).build()
                                                  : AmazonEC2ClientBuilder.standard().withRegion( awsCredentials.awsRegion() ).build();
    }

    private static AWSBatch awsBatch( AWSCredentials awsCredentials )
    {
        return awsCredentials.hasAwsCredentials() ? AWSBatchClientBuilder.standard()
                                                                         .withCredentials( awsCredentialsProvider( awsCredentials ) )
                                                                         .withRegion( awsCredentials.awsRegion() ).build()
                                                  : AWSBatchClientBuilder.standard().withRegion( awsCredentials.awsRegion() ).build();
    }

    private static AmazonCloudFormation amazonCloudFormation( AWSCredentials awsCredentials )
    {
        return awsCredentials.hasAwsCredentials() ? AmazonCloudFormationClientBuilder.standard()
                                                                                     .withCredentials( awsCredentialsProvider( awsCredentials ) )
                                                                                     .withRegion( awsCredentials.awsRegion() ).build()
                                                  : AmazonCloudFormationClientBuilder.standard().withRegion( awsCredentials.awsRegion() ).build();
    }

    private static AWSStaticCredentialsProvider awsCredentialsProvider( AWSCredentials awsCredentials )
    {
        return new AWSStaticCredentialsProvider( new BasicAWSCredentials( awsCredentials.awsAccessKeyId(), awsCredentials.awsSecretAccessKey() ) );
    }

    /**
     * Queries CloudFormation stack and returns found collection of {@link InfrastructureResources}.
     *
     * @param stackName name of the CloudFormation stack
     * @return collection of {@link InfrastructureResources}
     */
    public Collection<InfrastructureResource> findResources( String stackName )
    {
        DescribeStackResourcesResult describeStackResourcesResult =
                amazonCloudFormation.describeStackResources( new DescribeStackResourcesRequest().withStackName( stackName ) );

        List<StackResource> stackResources = describeStackResourcesResult.getStackResources();
        List<InfrastructureResource> infrastructureResources = new ArrayList<>();
        for ( StackResource stackResource : stackResources )
        {
            if ( "AWS::Batch::JobDefinition".equals( stackResource.getResourceType() ) )
            {
                InfrastructureResource infrastructureResource = getJobDefinitionResource( stackResource );
                infrastructureResources.add( infrastructureResource );
            }
            else if ( "AWS::Batch::JobQueue".equals( stackResource.getResourceType() ) )
            {
                InfrastructureResource infrastructureResource = getComputeEnvironmentResource( stackResources, stackResource );
                infrastructureResources.add( infrastructureResource );
            }
        }
        return infrastructureResources;
    }

    private InfrastructureResource getComputeEnvironmentResource( List<StackResource> stackResources, StackResource stackResource )
    {

        JobQueueDetail jobQueueDetail =
                awsBatch.describeJobQueues( new DescribeJobQueuesRequest().withJobQueues( stackResource.getPhysicalResourceId() ) )
                        .getJobQueues()
                        .stream()
                        .findFirst()
                        .orElseThrow( () -> new RuntimeException( format( "%s job queue not found", stackResource.getPhysicalResourceId() ) ) );
        ComputeEnvironmentOrder computeEnvironmentOrder =
                jobQueueDetail.getComputeEnvironmentOrder().stream().findFirst().orElseThrow( () -> new RuntimeException() );
        String computeEnvironment = computeEnvironmentOrder.getComputeEnvironment();

        ComputeEnvironmentDetail computeEnvironmentDetail = getComputeEnvironmentDetail( stackResources, computeEnvironment );

        InstanceTypeInfo instanceTypeInfo = getInstanceTypeInfo( computeEnvironmentDetail );
        return new ComputeEnvironmentResource( jobQueueDetail.getJobQueueName(),
                                               instanceTypeInfo.getVCpuInfo().getDefaultVCpus(),
                                               instanceTypeInfo.getMemoryInfo().getSizeInMiB(),
                                               instanceTypeInfo.getInstanceType() );
    }

    private ComputeEnvironmentDetail getComputeEnvironmentDetail( List<StackResource> stackResources, String computeEnvironment )
    {
        StackResource computeEnvironmentResource = stackResources.stream()
                                                                 .filter( resource -> resource.getPhysicalResourceId().equals( computeEnvironment ) )
                                                                 .findFirst()
                                                                 .orElseThrow(
                                                                         () -> new RuntimeException(
                                                                                 format( "%s compute environment not found", computeEnvironment ) ) );
        return awsBatch.describeComputeEnvironments(
                new DescribeComputeEnvironmentsRequest().withComputeEnvironments( computeEnvironmentResource.getPhysicalResourceId() ) )
                       .getComputeEnvironments()
                       .stream()
                       .findFirst()
                       .orElseThrow( () -> new RuntimeException( format( "resource %s not found", computeEnvironmentResource ) ) );
    }

    private InstanceTypeInfo getInstanceTypeInfo( ComputeEnvironmentDetail computeEnvironmentDetail )
    {
        String instanceType =
                computeEnvironmentDetail.getComputeResources()
                                        .getInstanceTypes()
                                        .stream()
                                        .findFirst()
                                        .orElseThrow( () -> new RuntimeException( format( "resource %s not found", computeEnvironmentDetail ) ) );
        return amazonEC2.describeInstanceTypes( new DescribeInstanceTypesRequest().withInstanceTypes( instanceType ) )
                        .getInstanceTypes()
                        .stream()
                        .findFirst()
                        .orElseThrow( () -> new RuntimeException( format( "instance type %s not found", instanceType ) ) );
    }

    private InfrastructureResource getJobDefinitionResource( StackResource stackResource )
    {
        DescribeJobDefinitionsResult describeJobDefinitionsResult = null;

        int retries = 0;
        boolean retry = false;
        int maxRetries = 10;

        do
        {
            long waitTime = getWaitTimeExp( retries );
            try
            {
                // Wait for the result.
                Thread.sleep( waitTime );

                describeJobDefinitionsResult =
                        awsBatch.describeJobDefinitions( new DescribeJobDefinitionsRequest().withJobDefinitions( stackResource.getPhysicalResourceId() ) );
            }
            catch ( AWSBatchException e )
            {
                if ( e.getErrorCode().equals( "429" ) )
                {
                    retry = true;
                }
                else
                {
                    throw e;
                }
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
        while ( retry && (retries++ < maxRetries) );

        return describeJobDefinitionsResult.getJobDefinitions()
                                           .stream()
                                           .findFirst()
                                           .map( InfrastructureResources::toJobDefinitionResource )
                                           .orElseThrow( () -> new RuntimeException(
                                                   format( "%s job definition not found", stackResource.getPhysicalResourceId() ) ) );
    }

    public static long getWaitTimeExp( int retryCount )
    {
        if ( 0 == retryCount )
        {
            return 0;
        }

        long waitTime = ((long) Math.pow( 2, retryCount ) * 100L);

        return waitTime;
    }

    private static JobDefinitionResource toJobDefinitionResource( JobDefinition jobDefinition )
    {

        String image = jobDefinition.getContainerProperties().getImage();
        Matcher matches = JDK_VERSION_PATTERN.matcher( image );
        if ( matches.find() )
        {
            return new JobDefinitionResource( jobDefinition.getJobDefinitionName(),
                                              jobDefinition.getContainerProperties().getVcpus(),
                                              jobDefinition.getContainerProperties().getMemory(),
                                              matches.group( 1 ),
                                              matches.group( 2 ) );
        }
        else
        {
            throw new IllegalArgumentException( format( "%s image name doesn't have expected jdk suffix", image ) );
        }
    }
}
