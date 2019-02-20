/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.util.Resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class GetBenchmarkGroups implements Query<List<BenchmarkGroup>>
{
    private static final String BENCHMARKS_FOR_GROUP = Resources.fileToString( "/queries/read/benchmark_groups_for_series.cypher" );

    private final String neo4jSeries;

    public GetBenchmarkGroups( String neo4jSeries )
    {
        this.neo4jSeries = neo4jSeries;
    }

    @Override
    public List<BenchmarkGroup> execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "neo4j_series", neo4jSeries );
            StatementResult result = session.run( BENCHMARKS_FOR_GROUP, params );
            return result.list().stream()
                         .map( r -> r.get( "bg" ).get( BenchmarkGroup.NAME ).asString() )
                         .map( BenchmarkGroup::new )
                         .collect( toList() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving benchmark groups", e );
        }
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return format( "%s(%s)", getClass().getSimpleName(), neo4jSeries );
    }
}
