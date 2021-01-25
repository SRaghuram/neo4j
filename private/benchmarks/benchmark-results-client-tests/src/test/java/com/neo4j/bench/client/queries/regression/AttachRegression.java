/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.regression;

import com.google.common.base.Joiner;
import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Regression;
import com.neo4j.bench.model.model.TestRun;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

import static com.neo4j.bench.model.util.MapPrinter.prettyPrint;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AttachRegression implements Query<Void>
{
    private static final String ATTACH_REGRESSION = Resources.fileToString( "/queries/regressions/attach_regression.cypher" );

    private final TestRun testRun;
    private final Regression regression;
    private final Benchmark benchmark;
    private final BenchmarkGroup benchmarkGroup;

    public AttachRegression(
            TestRun testRun,
            Benchmark benchmark,
            BenchmarkGroup benchmarkGroup,
            Regression regression )
    {
        this.testRun = requireNonNull( testRun );
        this.regression = requireNonNull( regression );
        this.benchmark = requireNonNull( benchmark );
        this.benchmarkGroup = requireNonNull( benchmarkGroup );
    }

    @Override
    public Void execute( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            Result statementResult = session.run( ATTACH_REGRESSION, toParams() );
            int regressionsCreated = statementResult.consume().counters().nodesCreated();
            if ( 1 != regressionsCreated )
            {
                throw new RuntimeException(
                        format( "Expected to create 1 regression but created %s\n" +
                                "Failed to create %s for:\n" +
                                " * Test run '%s'\n" +
                                " * Benchmark '%s'\n" +
                                " * Benchmark group '%s'",
                                regressionsCreated,
                                regression,
                                testRun,
                                benchmark,
                                benchmarkGroup ) );
            }
        }
        return null;
    }

    private Map<String,Object> toParams()
    {
        return new HashMap<String,Object>()
        {{
            put( "test_run_id", testRun.id() );
            put( "benchmark_name", benchmark.name() );
            put( "benchmark_group_name", benchmarkGroup.name() );
            put( "regression", regression.toParams() );
        }};
    }

    @Override
    public String toString()
    {
        return String.format( "AttachRegression { %s }, query=%s", Joiner.on( ", " ).withKeyValueSeparator( "=" ).join( toParams() ), ATTACH_REGRESSION );
    }

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
