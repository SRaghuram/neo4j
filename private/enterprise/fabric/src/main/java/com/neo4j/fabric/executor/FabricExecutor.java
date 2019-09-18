/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.eval.Catalog;
import com.neo4j.fabric.eval.FromEvaluation;
import com.neo4j.fabric.planner.api.Plan;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.planning.FabricQuery;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.transaction.FabricTransaction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static scala.collection.JavaConverters.asJavaIterable;
import static scala.collection.JavaConverters.seqAsJavaList;

public class FabricExecutor
{

    private final FabricConfig config;
    private final FabricPlanner planner;
    private final FromEvaluation fromEvaluation;

    public FabricExecutor( FabricConfig config, FabricPlanner planner, FromEvaluation fromEvaluation )
    {
        this.config = config;
        this.planner = planner;
        this.fromEvaluation = fromEvaluation;
    }

    public StatementResult run( FabricTransaction fabricTransaction, String statement, MapValue params )
    {
        FabricQuery query = planner.plan( statement, params );
        return fabricTransaction.execute( ctx ->
        {
            StatementResult start = StatementResults.initial();
            return run( query, params, start, ctx );
        } );
    }

    public boolean isPeriodicCommit( String query )
    {
        return planner.isPeriodicCommit( query );
    }

    private StatementResult run( FabricQuery query, MapValue params, StatementResult input, FabricTransaction.FabricExecutionContext ctx )
    {
        if ( query instanceof FabricQuery.LocalQuery )
        {
            return runLocalQuery( (FabricQuery.LocalQuery) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.ShardQuery )
        {
            return runShardQuery( (FabricQuery.ShardQuery) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.ChainedQuery )
        {
            return runChainedQuery( (FabricQuery.ChainedQuery) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.UnionQuery )
        {
            return runUnionQuery( (FabricQuery.UnionQuery) query, params, input, ctx );
        }
        else
        {
            throw notImplemented( "Unsupported query", query );
        }
    }

    private StatementResult runLocalQuery( FabricQuery.LocalQuery query, MapValue params, StatementResult input, FabricTransaction.FabricExecutionContext ctx )
    {
        return ctx.getLocal().run( query.query(), params, input );
    }

    private StatementResult runShardQuery( FabricQuery.ShardQuery query, MapValue params, StatementResult input, FabricTransaction.FabricExecutionContext ctx )
    {
        String queryString = query.queryString();
        Plan.QueryTask.QueryMode queryMode = getMode( query );
        Flux<Record> flux = input.records().flatMap( inputRecord ->
        {
            FabricConfig.Graph graph = evalFrom( query, params, inputRecord );

            StatementResult result = ctx.getRemote().run( graph, queryString, queryMode, params ).block();
            return result.records().map( rec -> Records.join( inputRecord, rec ) );
        } );

        return StatementResults.create( Flux.fromIterable( asJavaIterable( query.columns().produced() ) ), flux, Mono.empty() );
    }

    private FabricConfig.Graph evalFrom( FabricQuery.ShardQuery query, MapValue params, Record inputRecord )
    {
        Map<String,AnyValue> context = Records.asMap( inputRecord, seqAsJavaList( query.columns().input() ) );
        Catalog.Graph graph = fromEvaluation.evaluate( query.from(), params, context );
        if ( graph instanceof Catalog.RemoteGraph )
        {
            return ((Catalog.RemoteGraph) graph).graph();
        }
        else
        {
            throw notImplemented( "Graph was not a ShardGraph", graph.toString() );
        }
    }

    private StatementResult runChainedQuery( FabricQuery.ChainedQuery query, MapValue params, StatementResult input,
            FabricTransaction.FabricExecutionContext ctx )
    {
        StatementResult previous = input;
        for ( FabricQuery q : asJavaIterable( query.queries() ) )
        {
            previous = run( q, params, previous, ctx );
        }

        return previous;
    }

    private StatementResult runUnionQuery( FabricQuery.UnionQuery query, MapValue params, StatementResult input, FabricTransaction.FabricExecutionContext ctx )
    {
        StatementResult lhs = run( query.lhs(), params, input, ctx );
        StatementResult rhs = run( query.rhs(), params, input, ctx );
        Flux<Record> merged = Flux.merge( lhs.records(), rhs.records() );
        if ( query.distinct() )
        {
            merged = merged.distinct();
        }
        return StatementResults.create( rhs.columns(), merged, rhs.summary() );
    }

    private UnsupportedOperationException notImplemented( String msg, FabricQuery query )
    {
        return notImplemented( msg, query.toString() );
    }

    private UnsupportedOperationException notImplemented( String msg, String info )
    {
        return new UnsupportedOperationException( msg + ": " + info );
    }

    private Plan.QueryTask.QueryMode getMode( FabricQuery.ShardQuery query )
    {
        if ( query.query().part().containsUpdates() )
        {
            return Plan.QueryTask.QueryMode.CAN_READ_WRITE;
        }
        else
        {
            return Plan.QueryTask.QueryMode.CAN_READ_ONLY;
        }
    }
}
