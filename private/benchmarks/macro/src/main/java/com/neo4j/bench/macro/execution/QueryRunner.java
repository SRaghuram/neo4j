package com.neo4j.bench.macro.execution;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.InternalProfiler;
import com.neo4j.bench.client.results.ForkDirectory;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.Options.ExecutionMode;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;

import java.util.List;

public interface QueryRunner
{
    static QueryRunner runnerFor( ExecutionMode executionMode )
    {
        switch ( executionMode )
        {
        case EXECUTE:
            return new EmbeddedCypherRunner();
        case PLAN:
            return new EmbeddedCypherPlanner();
        default:
            throw new RuntimeException( "Unsupported execution mode: " + executionMode );
        }
    }

    void run( Jvm jvm,
              Store store,
              Edition edition,
              Neo4jConfig neo4jConfig,
              List<InternalProfiler> profilers,
              Query query,
              ForkDirectory forkDirectory,
              MeasurementControl warmupControl,
              MeasurementControl measurementControl );
}
