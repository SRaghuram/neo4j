/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.google.common.collect.Streams;
import com.neo4j.bench.infra.BenchmarkArgs;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class AWSBatchJobScheduler implements JobScheduler
{

    private static final Logger LOG = LoggerFactory.getLogger( AWSBatchJobScheduler.class );

    public static JobScheduler create( String region, String awsKey, String awsSecret, String jobQueue, String jobDefinition )
    {
        Objects.requireNonNull( awsKey );
        Objects.requireNonNull( awsSecret );
        return new AWSBatchJobScheduler( AWSBatchClientBuilder.standard()
                    .withCredentials( new AWSStaticCredentialsProvider( new BasicAWSCredentials( awsKey, awsSecret ) ) )
                    .withRegion( region )
                    .build(),
                    jobQueue,
                    jobDefinition
                );
    }

    private final AWSBatch awsBatch;
    private final String jobDefinition;
    private final String jobQueue;

    public AWSBatchJobScheduler( AWSBatch awsBatch, String jobQueue, String jobDefinition )
    {
        this.awsBatch = awsBatch;
        this.jobQueue = jobQueue;
        this.jobDefinition = jobDefinition;
    }

    @Override
    public List<String> schedule(
            String workloads,
            String dbs,
            BenchmarkArgs args )
    {
        List<WorkloadAndDb> workloadsAndDbs = Streams
                .zip(
                        Arrays.stream( workloads.split( "," ) ).map( String::trim ),
                        Arrays.stream( dbs.split( "," ) ).map( String::trim ),
                        WorkloadAndDb::new
                )
                .collect( toList() );

        return schedule( toSubmitJobRequest( workloadsAndDbs, args ) );
    }

    @Override
    public List<JobStatus> jobsStatuses( List<String> jobIds )
    {
        List<JobStatus> jobsStatuses = awsBatch.describeJobs( new DescribeJobsRequest().withJobs( jobIds ) )
            .getJobs()
            .stream()
            .map(JobStatus::from)
            .collect( Collectors.toList() );
        LOG.info( "current jobs statuses:\n{}", jobsStatuses.stream().map(Object::toString).collect( joining( "\n" ) ) );
        return jobsStatuses;
    }

    private List<String> schedule( List<SubmitJobRequest> submitJobRequests )
    {
        return submitJobRequests.stream()
                .map( awsBatch::submitJob )
                .map( SubmitJobResult::getJobId )
                .collect( toList() );
    }

    private List<SubmitJobRequest> toSubmitJobRequest(
            List<WorkloadAndDb> workloadsAndDbs,
            BenchmarkArgs args )
    {
        return workloadsAndDbs.stream()
        .map(workloadAndDb ->
        {
            String jobName = getJobName(workloadAndDb.workload);
            return new SubmitJobRequest()
                       .withJobDefinition( jobDefinition )
                       .withJobQueue( jobQueue )
                       .withJobName( jobName )
                       .withParameters( args.toJobParameters( workloadAndDb.workload, workloadAndDb.db ) );
        }).collect( toList() );
    }

    private static String getJobName( String workload )
    {
        return String.format( "macro-%s", workload );
    }

    private class WorkloadAndDb
    {
        private final String workload;
        private final String db;

        WorkloadAndDb( String workload, String db )
        {
            super();
            this.workload = workload;
            this.db = db;
        }
    }
}
