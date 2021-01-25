/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.deployment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awscdk.core.CfnOutput;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.batch.AllocationStrategy;
import software.amazon.awscdk.services.batch.ComputeEnvironment;
import software.amazon.awscdk.services.batch.ComputeResourceType;
import software.amazon.awscdk.services.batch.ComputeResources;
import software.amazon.awscdk.services.batch.JobDefinition;
import software.amazon.awscdk.services.batch.JobDefinitionContainer;
import software.amazon.awscdk.services.batch.JobQueue;
import software.amazon.awscdk.services.batch.JobQueueComputeEnvironment;
import software.amazon.awscdk.services.ec2.IMachineImage;
import software.amazon.awscdk.services.ec2.ISecurityGroup;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.LookupMachineImageProps;
import software.amazon.awscdk.services.ec2.MachineImage;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.Protocol;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.EcrImage;
import software.amazon.awscdk.services.ecs.Host;
import software.amazon.awscdk.services.ecs.MountPoint;
import software.amazon.awscdk.services.ecs.Ulimit;
import software.amazon.awscdk.services.ecs.UlimitName;
import software.amazon.awscdk.services.ecs.Volume;
import software.amazon.awscdk.services.iam.CfnInstanceProfile;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.neo4j.bench.infra.InfraNamesHelper.sanitizeDockerImageTag;
import static com.neo4j.bench.infra.InfraNamesHelper.sanitizeJobDefinitionName;
import static com.neo4j.bench.infra.InfraNamesHelper.sanitizeJobQueueName;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class BatchStack extends Stack
{

    public static final ImmutableList<String> BENCHMARKING_S3_BUCKET_RESOURCES = ImmutableList.of(
            "arn:aws:s3:::benchmarking.neo4j.com",
            "arn:aws:s3:::benchmarking.neo4j.com/*"
    );

    private final IVpc vpc;
    private final ISecurityGroup workerSecurityGroup;
    private final ISecurityGroup defaultSecurityGroup;
    private final IRole serviceRole;
    private final IMachineImage machineImage;
    private final CfnInstanceProfile instanceProfile;
    private final Map<String,EcrImage> workerImages;
    private final Map<InstanceType,MemAndCpus> instanceTypeInfos;

    BatchStack( final Construct scope, final String id, final StackProps props, BatchInfrastructure batchInfrastructure )
    {
        super( scope, id, props );

        String amiName = requireNonNull( (String) getNode().tryGetContext( "ami-name" ), "ami-name context variable missing" );

        vpc = Vpc.fromLookup( this, batchInfrastructure.vpc(),
                              VpcLookupOptions.builder()
                                              .isDefault( true )
                                              .build() );

        defaultSecurityGroup = SecurityGroup.fromSecurityGroupId( this, "DefaultSecurityGroup", batchInfrastructure.defaultSecurityGroup() );
        workerSecurityGroup = createWorkerSecurityGroup();

        serviceRole = createBatchServiceRole();

        instanceProfile = createInstanceProfile( batchInfrastructure );

        machineImage = MachineImage.lookup( LookupMachineImageProps.builder()
                                                                   .name( amiName )
                                                                   .build() );

        workerImages = findWorkerImages( batchInfrastructure );

        instanceTypeInfos = findInstanceTypeInfos( batchInfrastructure );

        for ( Map.Entry<String,BatchEnvironment> entry : batchInfrastructure.batchEnvironments().entrySet() )
        {
            BatchEnvironment batchEnvironment = entry.getValue();
            String jobQueueName = createBenchmarkingEnvironment( batchEnvironment.instanceType(), batchEnvironment.vcpus() );

            for ( String jdk : batchEnvironment.jdks() )
            {
                createJobDefinition( batchEnvironment.instanceType(), jdk );
            }

            CfnOutput.Builder.create( this, entry.getKey() )
                             .value( jobQueueName )
                             .build();
        }
    }

    private SecurityGroup createWorkerSecurityGroup()
    {
        SecurityGroup workerSecurityGroup = SecurityGroup.Builder.create( this, "BenchmarkingWorkerSecurityGroup" )
                                                                 .description( "Allow ssh into benchmarking worker instances" )
                                                                 .vpc( vpc )
                                                                 .build();

        workerSecurityGroup.addIngressRule( Peer.ipv4( "0.0.0.0/0" ),
                                            Port.Builder.create()
                                                        .fromPort( 22 )
                                                        .toPort( 22 )
                                                        .protocol( Protocol.TCP )
                                                        .stringRepresentation( "incoming ssh connections" )
                                                        .build() );
        return workerSecurityGroup;
    }

    private IRole createBatchServiceRole()
    {
        final IRole serviceRole;
        Map<String,PolicyDocument> batchServiceRoleInlinePolicy = ImmutableMap.of(
                nameWithEnvSuffix( "access-to-benchmarking-artifacts" ),
                PolicyDocument.Builder.create()
                                      .assignSids( true )
                                      .statements( ImmutableList.of(
                                              PolicyStatement.Builder.create()
                                                                     .effect( Effect.ALLOW )
                                                                     .actions( ImmutableList.of(
                                                                             "s3:PutObject",
                                                                             "s3:GetObject",
                                                                             "s3:ListBucket"
                                                                     ) )
                                                                     .resources( BENCHMARKING_S3_BUCKET_RESOURCES )
                                                                     .build(),
                                              PolicyStatement.Builder.create()
                                                                     .effect( Effect.ALLOW )
                                                                     .actions( ImmutableList.of(
                                                                             "ecr:GetLifecyclePolicyPreview",
                                                                             "ecr:GetDownloadUrlForLayer",
                                                                             "ecr:BatchGetImage",
                                                                             "ecr:DescribeImages",
                                                                             "ecr:GetAuthorizationToken",
                                                                             "ecr:ListTagsForResource",
                                                                             "ecr:BatchCheckLayerAvailability",
                                                                             "ecr:GetRepositoryPolicy",
                                                                             "ecr:GetLifecyclePolicy"
                                                                     ) )
                                                                     .resources( ImmutableList.of( "*" ) )
                                                                     .build() ) )
                                      .build() );

        serviceRole = Role.Builder.create( this, "BatchServiceRole" )
                                  .managedPolicies( ImmutableList.of(
                                          ManagedPolicy.fromAwsManagedPolicyName( "service-role/AWSBatchServiceRole" ) ) )
                                  .inlinePolicies( batchServiceRoleInlinePolicy )
                                  .assumedBy( ServicePrincipal.Builder.create( "batch.amazonaws.com" ).build() )
                                  .build();
        return serviceRole;
    }

    private CfnInstanceProfile createInstanceProfile( BatchInfrastructure batchInfrastructure )
    {
        final CfnInstanceProfile instanceProfile;
        Map<String,PolicyDocument> ecsInstanceRoleInlinePolicy = ImmutableMap.of(
                nameWithEnvSuffix( "access-batch-compute-environment" ),
                PolicyDocument.Builder.create()
                                      .assignSids( true )
                                      .statements( ImmutableList.of(
                                              PolicyStatement.Builder.create()
                                                                     .effect( Effect.ALLOW )
                                                                     .actions( ImmutableList.of(
                                                                             "batch:DescribeComputeEnvironments"
                                                                     ) )
                                                                     .resources( ImmutableList.of( "*" ) )
                                                                     .build()
                                      ) )
                                      .build(),
                nameWithEnvSuffix( "access-benchmarking-artifacts" ),
                PolicyDocument.Builder.create()
                                      .assignSids( true )
                                      .statements( ImmutableList.of(
                                              PolicyStatement.Builder.create()
                                                                     .effect( Effect.ALLOW )
                                                                     .actions( ImmutableList.of(
                                                                             "s3:PutObject",
                                                                             "s3:GetObject",
                                                                             "s3:GetLifecycleConfiguration",
                                                                             "s3:PutLifecycleConfiguration",
                                                                             "s3:ListBucket"
                                                                     ) )
                                                                     .resources( BENCHMARKING_S3_BUCKET_RESOURCES )
                                                                     .build()
                                      ) )
                                      .build(),
                nameWithEnvSuffix( "access-results-db-store-password" ),
                PolicyDocument.Builder.create()
                                      .assignSids( true )
                                      .statements( ImmutableList.of(
                                              PolicyStatement.Builder.create()
                                                                     .effect( Effect.ALLOW )
                                                                     .actions( ImmutableList.of(
                                                                             "secretsmanager:GetSecretValue"
                                                                     ) )
                                                                     .resources( batchInfrastructure.resultStoreSecrets() )
                                                                     .build()
                                      ) )
                                      .build()
        );

        IRole ecsInstanceRole = Role.Builder.create( this, "ecsInstanceRole" )
                                            .managedPolicies( ImmutableList.of(
                                                    ManagedPolicy.fromAwsManagedPolicyName( "service-role/AmazonEC2ContainerServiceforEC2Role" ) ) )
                                            .inlinePolicies( ecsInstanceRoleInlinePolicy )
                                            .assumedBy( ServicePrincipal.Builder.create( "ec2.amazonaws.com" ).build() )
                                            .build();

        instanceProfile = CfnInstanceProfile.Builder.create( this, "instanceProfile" )
                                                    .roles( ImmutableList.of( ecsInstanceRole.getRoleName() ) )
                                                    .build();
        return instanceProfile;
    }

    private Map<InstanceType,MemAndCpus> findInstanceTypeInfos( BatchInfrastructure batchInfrastructure )
    {
        Set<String> instanceTypes = batchInfrastructure.batchEnvironments()
                                                       .values()
                                                       .stream()
                                                       .map( BatchEnvironment::instanceType )
                                                       .map( InstanceType::toString )
                                                       .collect( toSet() );
        Ec2Client ec2Client = Ec2Client.builder().build();
        return ec2Client.describeInstanceTypes( DescribeInstanceTypesRequest.builder()
                                                                            .instanceTypesWithStrings( instanceTypes )
                                                                            .build() )
                        .instanceTypes()
                        .stream()
                        .collect( toMap( InstanceTypeInfo::instanceType,
                                         instanceTypeInfo -> new MemAndCpus(
                                                 instanceTypeInfo.memoryInfo().sizeInMiB(),
                                                 instanceTypeInfo.vCpuInfo().defaultVCpus()
                                         ) ) );
    }

    private Map<String,EcrImage> findWorkerImages( BatchInfrastructure batchInfrastructure )
    {
        IRepository repository = Repository.fromRepositoryName( this, "WorkerImages", "benchmarks-worker" );

        return batchInfrastructure.batchEnvironments()
                                  .values()
                                  .stream()
                                  .map( BatchEnvironment::jdks )
                                  .flatMap( Set::stream )
                                  .distinct()
                                  .collect( toMap( Function.identity(),
                                                   jdk -> ContainerImage.fromEcrRepository( repository, sanitizeDockerImageTag( jdk ) ) ) );
    }

    private JobDefinition createJobDefinition( InstanceType instanceType, String jdk )
    {

        String resource = format( "%s-%s", instanceType.toString(), jdk );
        String jobDefinitionId = resourceId( "JobDefinition", resource );
        String jobDefinitionName = sanitizeJobDefinitionName( nameWithEnvSuffix( resource ) );

        EcrImage ecrImage = workerImages.get( jdk );
        MemAndCpus memAndCpus = instanceTypeInfos.get( instanceType );
        int vcpus = memAndCpus.vCpus;
        long memoryLimitMiB = memAndCpus.sizeInMiB - 1024;

        return JobDefinition.Builder.create( this, jobDefinitionId )
                                    .jobDefinitionName( jobDefinitionName )
                                    .container( JobDefinitionContainer.builder()
                                                                      .command( asList(
                                                                              "/work/bootstrap-worker.sh",
                                                                              "--worker-artifact-uri",
                                                                              "Ref::--worker-artifact-uri",
                                                                              "run-worker",
                                                                              "--artifact-base-uri",
                                                                              "Ref::--artifact-base-uri" ) )
                                                                      .image( ecrImage )
                                                                      .volumes( singletonList(
                                                                              Volume.builder()
                                                                                    .host( Host.builder()
                                                                                               .sourcePath( "/work" )
                                                                                               .build() )
                                                                                    .name( "work" )
                                                                                    .build() ) )
                                                                      .mountPoints( singletonList(
                                                                              MountPoint.builder()
                                                                                        .sourceVolume( "work" )
                                                                                        .containerPath( "/work/run" )
                                                                                        .readOnly( false )
                                                                                        .build() ) )
                                                                      .privileged( true )
                                                                      .vcpus( vcpus )
                                                                      .memoryLimitMiB( memoryLimitMiB )
                                                                      .ulimits( singletonList(
                                                                              Ulimit.builder()
                                                                                    .name( UlimitName.NOFILE )
                                                                                    .hardLimit( 80000 )
                                                                                    .softLimit( 80000 )
                                                                                    .build() ) )
                                                                      .build() )
                                    .build();
    }

    private String createBenchmarkingEnvironment( InstanceType instanceType, int maxvCpus )
    {
        ComputeResources computeResources = ComputeResources.builder()
                                                            .vpc( vpc )
                                                            .securityGroups( singletonList( workerSecurityGroup ) )
                                                            .type( ComputeResourceType.SPOT )
                                                            .allocationStrategy( AllocationStrategy.SPOT_CAPACITY_OPTIMIZED )
                                                            .maxvCpus( maxvCpus )
                                                            .image( machineImage )
                                                            .instanceTypes( singletonList(
                                                                    // TODO: FQN class is required as there is clash between AWS SDK version
                                                                    new software.amazon.awscdk.services.ec2.InstanceType( instanceType.toString() ) ) )
                                                            .instanceRole( instanceProfile.getAttrArn() )
                                                            .securityGroups( ImmutableList.of( workerSecurityGroup, defaultSecurityGroup ) )
                                                            .build();

        String computeEnvironmentId = resourceId( "ComputeEnvironment", instanceType.toString() );
        String computeEnvironmentName = sanitizeJobDefinitionName( nameWithEnvSuffix( instanceType.toString() ) );
        ComputeEnvironment computeEnvironment = ComputeEnvironment.Builder
                .create( this, computeEnvironmentId )
                .computeEnvironmentName( computeEnvironmentName )
                .managed( true )
                .serviceRole( serviceRole )
                .computeResources( computeResources )
                .build();

        String jobQueueId = resourceId( "JobQueue", instanceType.toString() );
        String jobQueueName = sanitizeJobQueueName( nameWithEnvSuffix( instanceType.toString() ) );
        JobQueue.Builder.create( this, jobQueueId )
                        .jobQueueName( jobQueueName )
                        .computeEnvironments( singletonList( JobQueueComputeEnvironment.builder()
                                                                                       .computeEnvironment( computeEnvironment )
                                                                                       .order( 1 )
                                                                                       .build() ) )
                        .build();

        return jobQueueName;
    }

    private String nameWithEnvSuffix( String name )
    {
        return requireNonNull( name ) + "-" + getStackName();
    }

    private String resourceId( String resourceType, String resource )
    {
        return format( "%s-%s", resourceType, resource );
    }

    private static class MemAndCpus
    {
        private final Long sizeInMiB;
        private final Integer vCpus;

        MemAndCpus( Long sizeInMiB, Integer vCpus )
        {
            this.sizeInMiB = sizeInMiB;
            this.vCpus = vCpus;
        }
    }
}
