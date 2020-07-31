/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.compositeOf;
import static java.util.stream.Collectors.toList;

public abstract class QueryRunner
{
    public static QueryRunner queryRunnerFor( ExecutionMode executionMode,
                                              Function<ForkDirectory,Database> databaseCreator )
    {
        switch ( executionMode )
        {
        case EXECUTE:
            return new CypherExecutingRunner( databaseCreator );
        case PLAN:
            return new CypherPlanningRunner( databaseCreator );
        default:
            throw new RuntimeException( "Unsupported execution mode: " + executionMode );
        }
    }

    /**
     * NOTE: this command does not export summary of results as JSON. This is because it has two purposes and neither of them requires it:
     * (1) Interactive execution, locally by developers. Result logs and console output should suffice.
     * (2) Invoked from 'run-workload'. Multiple forks for same query, so result log merging needs to happen before constructing summary.
     */
    public static void runSingleCommand( QueryRunner queryRunner,
                                         Jvm jvm,
                                         ForkDirectory forkDir,
                                         Workload workload,
                                         String queryName,
                                         Planner planner,
                                         Runtime runtime,
                                         ExecutionMode executionMode,
                                         Map<Pid,Parameters> pidParameters,
                                         Map<Pid,List<ProfilerType>> pidProfilers,
                                         int warmupCount,
                                         int minMeasurementSeconds,
                                         int maxMeasurementSeconds,
                                         int measurementCount )
    {

        Query query = workload.queryForName( queryName )
                              .copyWith( planner )
                              .copyWith( runtime )
                              .copyWith( executionMode );

        List<ProfilerType> allProfilerTypes = pidProfilers.keySet().stream()
                                                          .map( pidProfilers::get )
                                                          .flatMap( List::stream )
                                                          .distinct()
                                                          .collect( toList() );
        ProfilerType.assertInternal( allProfilerTypes );

        Map<Pid,List<InternalProfiler>> pidInternalProfilers = new HashMap<>();
        pidProfilers.keySet().forEach( pid -> pidInternalProfilers.put( pid, ProfilerType.createInternalProfilers( pidProfilers.get( pid ) ) ) );

        queryRunner.run( jvm,
                         pidParameters,
                         pidInternalProfilers,
                         query,
                         forkDir,
                         compositeOf( warmupCount, minMeasurementSeconds, maxMeasurementSeconds ),
                         compositeOf( measurementCount, minMeasurementSeconds, maxMeasurementSeconds ) );
    }

    protected abstract void run( Jvm jvm,
                                 Map<Pid,Parameters> pidParameters,
                                 Map<Pid,List<InternalProfiler>> pidProfilers,
                                 Query query,
                                 ForkDirectory forkDirectory,
                                 MeasurementControl warmupControl,
                                 MeasurementControl measurementControl );
}
