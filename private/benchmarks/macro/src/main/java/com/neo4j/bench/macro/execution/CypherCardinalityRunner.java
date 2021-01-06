/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.CardinalityMeasurement;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.model.model.Parameters;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.CARDINALITY;

public class CypherCardinalityRunner extends QueryRunner
{
    private final Function<ForkDirectory,Database> databaseCreator;

    CypherCardinalityRunner( Function<ForkDirectory,Database> databaseCreator )
    {
        this.databaseCreator = databaseCreator;
    }

    @Override
    protected void run( Jvm jvm,
                        Map<Pid,Parameters> pidParameters,
                        Map<Pid,List<InternalProfiler>> pidProfilers,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try ( Database database = databaseCreator.apply( forkDirectory ) )
        {
            double maxError = 1_000_000;
            MeasuringExecutor measuringExecutor = MeasuringExecutor.toMeasureCardinalityAccuracy( CardinalityMeasurement.geometricMean( maxError ),
                                                                                                  pidParameters,
                                                                                                  query );
            new Runner().run( jvm,
                              database,
                              pidParameters,
                              pidProfilers,
                              query.queryString(),
                              query.copyWith( CARDINALITY ).queryString(),
                              query.benchmarkGroup(),
                              query.benchmark(),
                              query.parameters().create(),
                              forkDirectory,
                              // no need to warm-up, as we are measuring accuracy rather than latency
                              MeasurementControl.none(),
                              measurementControl,
                              false,
                              measuringExecutor );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }
}
