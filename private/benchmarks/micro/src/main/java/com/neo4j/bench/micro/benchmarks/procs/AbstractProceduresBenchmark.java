/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.BasicContext;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;

@State( Scope.Benchmark )
public abstract class AbstractProceduresBenchmark extends BaseDatabaseBenchmark
{
    static final ResourceTracker DUMMY_TRACKER = new DummyResourceTracker();
    GlobalProcedures procedures;
    int token;
    Context context;

    @Override
    protected void afterDatabaseStart()
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db()).getDependencyResolver();
        procedures = dependencyResolver.resolveDependency( GlobalProcedures.class );
        context = BasicContext.buildContext( dependencyResolver,
                                             dependencyResolver.resolveDependency( ValueMapper.class ) ).context();
    }

    @Override
    public String description()
    {
        return "Test procedures, user-defined functions and user-defined aggregation functions";
    }

    @Override
    public String benchmarkGroup()
    {
        return "Procedures";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    protected static class DummyResourceTracker implements ResourceTracker
    {
        @Override
        public void registerCloseableResource( AutoCloseable autoCloseable )
        {
            // do nothing
        }

        @Override
        public void unregisterCloseableResource( AutoCloseable autoCloseable )
        {
            // do nothing
        }
    }
}
