/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.procs;

import com.neo4j.bench.micro.benchmarks.BaseDatabaseBenchmark;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.api.procedure.BasicContext;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

@State( Scope.Benchmark )
public abstract class AbstractProceduresBenchmark extends BaseDatabaseBenchmark
{
    GlobalProcedures procedures;
    int token;
    Context context;

    @Override
    protected void afterDatabaseStart( DataGeneratorConfig config )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db()).getDependencyResolver();
        procedures = dependencyResolver.resolveDependency( GlobalProcedures.class );
        context = BasicContext.buildContext( dependencyResolver, MAPPER ).context();
    }

    private static final ValueMapper<Object> MAPPER = new ValueMapper.JavaMapper()
    {
        @Override
        public Object mapPath( PathValue value )
        {
            throw new RuntimeException( "Unable to evaluate paths" );
        }

        @Override
        public Object mapNode( VirtualNodeValue value )
        {
            throw new RuntimeException( "Unable to evaluate nodes" );
        }

        @Override
        public Object mapRelationship( VirtualRelationshipValue value )
        {
            throw new RuntimeException( "Unable to evaluate relationships" );
        }
    };

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
}
