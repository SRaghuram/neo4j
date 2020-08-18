/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.submit;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Job;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;

import static org.neo4j.driver.Values.parameters;

public class CreateJob implements Query<Boolean>
{

    private static final String QUERY = Resources.fileToString( "/queries/write/create_job.cypher" );

    private final Job job;
    private final String testRunId;

    public CreateJob( Job job, String testRunId )
    {
        this.job = Objects.requireNonNull( job );
        this.testRunId = testRunId;
    }

    public Job job()
    {
        return job;
    }

    public String testRunId()
    {
        return testRunId;
    }

    @Override
    public Boolean execute( Driver driver )
    {
        return driver.session().writeTransaction( tx -> createBatchJob( tx ) );
    }

    private Boolean createBatchJob( org.neo4j.driver.Transaction tx )
    {
        Result result = tx.run(
                QUERY,
                parameters(
                        "job", job.asMap(),
                        "testRunId", testRunId
                ) );
        return result.consume().counters().nodesCreated() == 1;
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
