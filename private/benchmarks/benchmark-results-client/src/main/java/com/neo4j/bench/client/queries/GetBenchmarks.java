/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Benchmark.Mode;
import com.neo4j.bench.client.util.Resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class GetBenchmarks implements Query<List<Benchmark>>
{
    private static final String BENCHMARKS_FOR_GROUP = Resources.fileToString( "/queries/read/benchmarks_for_group_and_series.cypher" );

    private final BenchmarkGroup group;
    private final String neo4jSeries;
    private final String branchOwner;

    public GetBenchmarks( BenchmarkGroup group, String neo4jSeries, String branchOwner )
    {
        this.group = group;
        this.neo4jSeries = neo4jSeries;
        this.branchOwner = branchOwner;
    }

    @Override
    public List<Benchmark> execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "group", group.name() );
            params.put( "neo4j_series", neo4jSeries );
            params.put( "owner", branchOwner );
            StatementResult result = session.run( BENCHMARKS_FOR_GROUP, params );
            return result.list().stream()
                         .map( r -> Benchmark.benchmarkFor(
                                 r.get( "b" ).get( Benchmark.DESCRIPTION ).asString(),
                                 r.get( "b" ).get( Benchmark.NAME ).asString(),
                                 r.get( "b" ).get( Benchmark.SIMPLE_NAME ).asString(),
                                 Mode.valueOf( r.get( "b" ).get( Benchmark.MODE ).asString() ),
                                 r.get( "bp" ).asMap( Value::asString ) ) )
                         .collect( toList() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving benchmarks", e );
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
        return format( "%s(%s, %s, %s)", getClass().getSimpleName(), group.name(), neo4jSeries, branchOwner );
    }
}
