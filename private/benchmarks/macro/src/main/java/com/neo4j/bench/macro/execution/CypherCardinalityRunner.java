/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.CardinalityMeasurement;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.common.profiling.assist.InternalProfilerAssist;
import com.neo4j.bench.macro.workload.Query;

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
    protected void run( InternalProfilerAssist profilerAssist,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try ( Database database = databaseCreator.apply( forkDirectory ) )
        {
            double maxError = 1_000_000;
            MeasuringExecutor measuringExecutor = MeasuringExecutor.toMeasureCardinalityAccuracy( CardinalityMeasurement.geometricMean( maxError ),
                                                                                                  profilerAssist.ownParameter(),
                                                                                                  query );
            new Runner().run( database,
                              profilerAssist,
                              query.queryString(),
                              query.copyWith( CARDINALITY ).queryString(),
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
