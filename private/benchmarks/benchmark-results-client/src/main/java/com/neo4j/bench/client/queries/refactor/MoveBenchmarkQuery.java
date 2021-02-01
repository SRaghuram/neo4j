/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.refactor;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.BenchmarkGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.SummaryCounters;

import static org.neo4j.driver.Values.parameters;

public class MoveBenchmarkQuery implements Query<Void>
{
    private static final Logger LOG = LoggerFactory.getLogger( MoveBenchmarkQuery.class );

    private static final org.neo4j.driver.Query MOVE_GROUP = new org.neo4j.driver.Query( Resources.fileToString( "/queries/refactor/move_group.cypher" ) );

    private final org.neo4j.driver.Query query;

    public MoveBenchmarkQuery( String benchmarkToolName, BenchmarkGroup oldGroup, BenchmarkGroup newGroup, String benchmarkName )
    {
        Value parameters = parameters( "benchmark_tool_name", benchmarkToolName,
                                       "old_benchmark_group_name", oldGroup.name(),
                                       "new_benchmark_group_name", newGroup.name(),
                                       "benchmark_simple_name", benchmarkName );
        this.query = MOVE_GROUP.withUpdatedParameters( parameters );
    }

    @Override
    public Void execute( Driver driver )
    {
        return driver.session()
                     .writeTransaction( tx ->
                                        {
                                            runUpdate( tx, query );
                                            return null;
                                        } );
    }

    private void runUpdate( Transaction tx, org.neo4j.driver.Query query )
    {
        LOG.info( "Running update {}", query );
        Result result = tx.run( query );
        SummaryCounters counters = result.consume().counters();
        LOG.info( "Query executed: nodesCreated: {}, nodesDeleted: {}, relationshipsCreated: {}, relationshipsDeleted: {}",
                  counters.nodesCreated(), counters.nodesDeleted(), counters.relationshipsCreated(), counters.relationshipsDeleted() );
    }
}
