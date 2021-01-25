/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.process.HasPid;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.execution.measurement.CardinalityMeasurement;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.execution.measurement.Results;
import com.neo4j.bench.macro.execution.measurement.Results.MeasurementUnit;
import com.neo4j.bench.macro.workload.ParametersReader;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class MeasuringExecutor
{
    public static MeasuringExecutor toMeasureLatency()
    {
        return new MeasuringExecutor.LatencyRunner();
    }

    public static MeasuringExecutor toMeasureCardinalityAccuracy( CardinalityMeasurement cardinalityMeasurement,
                                                                  Map<Pid,Parameters> pidParameters,
                                                                  Query query )
    {
        Parameters additionalParameters = pidParameters.get( HasPid.getPid() );
        return new MeasuringExecutor.QErrorRunner( cardinalityMeasurement,
                                                   additionalParameters,
                                                   FullBenchmarkName.from( query.benchmarkGroup(), query.benchmark() ) );
    }

    abstract void execute( QueryString queryString,
                           ParametersReader parameters,
                           MeasurementControl measurementControl,
                           Database db,
                           ForkDirectory forkDirectory,
                           boolean shouldRollback,
                           RunPhase phase ) throws Exception;

    private static class LatencyRunner extends MeasuringExecutor
    {
        @Override
        protected void execute( QueryString queryString,
                                ParametersReader parameters,
                                MeasurementControl measurementControl,
                                Database db,
                                ForkDirectory forkDirectory,
                                boolean shouldRollback,
                                RunPhase phase ) throws Exception
        {
            String queryForThisIteration = queryString.value();
            Map<String,Object> queryParameters = null;
            boolean executeInTx = !queryString.isPeriodicCommit();
            try ( Results.ResultsWriter resultsWriter = Results.newWriter( forkDirectory, phase, MeasurementUnit.ofDuration( NANOSECONDS ) ) )
            {
                measurementControl.reset();
                while ( !measurementControl.isComplete() && parameters.hasNext() )
                {
                    queryForThisIteration = queryString.value();
                    queryParameters = parameters.next();
                    long startTimeUtc = System.currentTimeMillis();
                    long start = System.nanoTime();

                    int rowCount = db.executeAndGetRows( queryForThisIteration, queryParameters, executeInTx, shouldRollback );

                    long stop = System.nanoTime();
                    long duration = stop - start;
                    resultsWriter.write( startTimeUtc, startTimeUtc, duration, rowCount );
                    measurementControl.register( duration );
                }
            }
            // If the warm-up query needs to be rolled back (e.g., because mutating the graph would interfere with the measurement queries), then we need run
            // execute one more warmup-up query and commit it. This is not ideal, as it can leave the graph in an unknown state, but is difficult to get around,
            // because we need to commit at least one record of each kind that the benchmark writes, in order to bridge the ID gaps created by the rollbacks.
            // Running one measurement query before measurement was considered, but this is not reliable in the case that the measurement query is Single Shot.
            // See below for more details.
            //
            // About our ID generators: running transactions that rolls back instead of committing can't quite mark those IDs as reusable during rollback.
            // Reason is that rollbacks aren't recorded in the transaction log and therefore will not propagate over a cluster and i.e. would have resulted
            // in each cluster member ending up with differences in their ID freelists. Instead the next committing transaction will bridge any gap in high ID
            // from the previously committed transaction. Doing a lot of warmup w/o committing makes this gap very large, so large that bridging it becomes
            // a big part of the measurement, which is not what we want. Therefore the warmup session ends with one transaction that commits in the end.
            if ( shouldRollback )
            {
                db.executeAndGetRows( queryForThisIteration, queryParameters, executeInTx, false );
            }
            if ( !measurementControl.isComplete() )
            {
                throw new RuntimeException( "Run finished before it was supposed to, probably because it ran out of parameters" );
            }
        }
    }

    private static class QErrorRunner extends MeasuringExecutor
    {
        private final CardinalityMeasurement cardinalityMeasurement;
        private final Parameters additionalParameters;
        private final FullBenchmarkName fullBenchmarkName;
        private final Map<PlanOperator,Integer> aggregatedPlans;

        private QErrorRunner( CardinalityMeasurement cardinalityMeasurement,
                              Parameters additionalParameters,
                              FullBenchmarkName fullBenchmarkName )
        {
            this.cardinalityMeasurement = cardinalityMeasurement;
            this.additionalParameters = additionalParameters;
            this.fullBenchmarkName = fullBenchmarkName;
            this.aggregatedPlans = new HashMap<>();
        }

        @Override
        protected void execute( QueryString queryString,
                                ParametersReader parameters,
                                MeasurementControl measurementControl,
                                Database db,
                                ForkDirectory forkDirectory,
                                boolean shouldRollback,
                                RunPhase phase ) throws Exception
        {
            try ( Results.ResultsWriter resultsWriter = Results.newWriter( forkDirectory, phase, MeasurementUnit.ofCardinality() ) )
            {
                aggregatedPlans.clear();
                boolean executeInTx = !queryString.isPeriodicCommit();
                measurementControl.reset();
                while ( !measurementControl.isComplete() && parameters.hasNext() )
                {
                    String queryForThisIteration = queryString.value();
                    Map<String,Object> queryParameters = parameters.next();
                    PlanOperator plan = db.executeAndGetPlan( queryForThisIteration, queryParameters, executeInTx, shouldRollback );
                    addToPlanAggregates( plan );

                    double cardinalityScore = cardinalityMeasurement.calculateError( plan );
                    long rows = plan.rows().get();

                    long startTimeUtc = -1;
                    resultsWriter.write( startTimeUtc, startTimeUtc, cardinalityScore, rows );
                    measurementControl.register( cardinalityScore );
                }
            }
            if ( !measurementControl.isComplete() )
            {
                throw new RuntimeException( "Run finished before it was supposed to, probably because it ran out of parameters" );
            }

            // NOTE: only possible to attach one profiler recording of each type, so we select the most used plan to create that recording from
            Map.Entry<PlanOperator,Integer> mostUsed = aggregatedPlans.entrySet().stream()
                                                                      .max( Map.Entry.comparingByValue() )
                                                                      .orElseThrow( () -> new IllegalStateException(
                                                                              "Expected to find at least one plan" ) );

            PlanOperator mostUsedPlan = mostUsed.getKey();
            mostUsedPlan.divideProfiledCountsBy( mostUsed.getValue() );
            String asciiPlan = PlannerDescription.toAsciiPlan( mostUsedPlan );
            RecordingDescriptor recordingDescriptor = new RecordingDescriptor( fullBenchmarkName,
                                                                               phase,
                                                                               RecordingType.ASCII_PLAN,
                                                                               additionalParameters,
                                                                               Collections.emptySet(), /*no secondary benchmarks*/
                                                                               true /*every fork will produce plans recording*/ );
            Path asciiPlanRecordingFile = forkDirectory.registerPathFor( recordingDescriptor );
            Files.write( asciiPlanRecordingFile, asciiPlan.getBytes( StandardCharsets.UTF_8 ) );
        }

        private void addToPlanAggregates( PlanOperator planOperator )
        {
            Optional<PlanOperator> maybeAggregatePlan = getAggregatePlanFor( planOperator );
            if ( maybeAggregatePlan.isPresent() )
            {
                aggregatedPlans.computeIfPresent( maybeAggregatePlan.get(), ( aggregatePlan, count ) ->
                {
                    aggregatePlan.addProfiledCountsFrom( planOperator );
                    return count + 1;
                } );
            }
            else
            {
                aggregatedPlans.put( planOperator, 1 );
            }
        }

        private Optional<PlanOperator> getAggregatePlanFor( PlanOperator planOperator )
        {
            return aggregatedPlans.keySet().stream().filter( planOperator::isEquivalent ).findFirst();
        }
    }
}
