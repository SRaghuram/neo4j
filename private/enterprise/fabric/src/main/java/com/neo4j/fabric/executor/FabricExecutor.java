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
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.stream.SyncPublisher;
import com.neo4j.fabric.transaction.FabricTransaction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static scala.collection.JavaConverters.asJavaIterable;
import static scala.collection.JavaConverters.mapAsJavaMap;
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
        Flux<String> outputCols = Flux.fromIterable( asJavaIterable( query.columns().output() ) );
        Flux<Record> output = run( query, params, input.records(), ctx );
        return StatementResults.create( outputCols, output, Mono.empty() );
    }

    private Flux<Record> run( FabricQuery query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        if ( query instanceof FabricQuery.Direct )
        {
            return runDirectQuery( (FabricQuery.Direct) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.Apply )
        {
            return runApplyQuery( (FabricQuery.Apply) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.LocalQuery )
        {
            return runLocalQuery( (FabricQuery.LocalQuery) query, params, input, ctx );
        }
        else if ( query instanceof FabricQuery.RemoteQuery )
        {
            return runRemoteQuery( (FabricQuery.RemoteQuery) query, params, input, ctx );
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

    private Flux<Record> runChainedQuery( FabricQuery.ChainedQuery query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        Flux<Record> previous = input;
        for ( FabricQuery q : asJavaIterable( query.queries() ) )
        {
            previous = run( q, params, previous, ctx );
        }

        return previous;
    }

    private Flux<Record> runUnionQuery( FabricQuery.UnionQuery query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        Flux<Record> lhs = run( query.lhs(), params, input, ctx );
        Flux<Record> rhs = run( query.rhs(), params, input, ctx );
        Flux<Record> merged = Flux.merge( lhs, rhs );
        if ( query.distinct() )
        {
            merged = merged.distinct();
        }
        return merged;
    }

    private Flux<Record> runDirectQuery( FabricQuery.Direct query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        return run( query.query(), params, input, ctx );
    }

    private Flux<Record> runApplyQuery( FabricQuery.Apply query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        return input.flatMap( inputRecord ->
                run( query.query(), params, Flux.just( inputRecord ), ctx )
                        .map( outputRecord ->
                                Records.join( inputRecord, outputRecord ) ) );
    }

    private Flux<Record> runLocalQuery( FabricQuery.LocalQuery query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        // Wrap in StatementResult, REMOVE?
        Flux<String> inputCols = Flux.fromIterable( asJavaIterable( query.columns().local() ) );
        StatementResult inputStream = StatementResults.create( inputCols, input, Mono.empty() );
        return ctx.getLocal().run( query.query(), params, inputStream ).records();
    }

    private Flux<Record> runRemoteQuery( FabricQuery.RemoteQuery query, MapValue params, Flux<Record> input, FabricTransaction.FabricExecutionContext ctx )
    {
        String queryString = query.queryString();
        Plan.QueryTask.QueryMode queryMode = getMode( query );
        return input.flatMap( inputRecord ->
        {
            Map<String,AnyValue> recordValues = recordAsMap( query, inputRecord );
            FabricConfig.Graph graph = evalFrom( query, params, recordValues );
            MapValue parameters = addImportParams( params, recordValues, mapAsJavaMap( query.parameters() ) );

            // Wrapped in StatementResult, REMOVE?
            StatementResult statementResult = ctx.getRemote().run( graph, queryString, queryMode, parameters ).block();
            Rx2SyncStream syncStream = new Rx2SyncStream( statementResult,
                    config.getDataStream().getBufferLowWatermark(),
                    config.getDataStream().getBufferSize(),
                    config.getDataStream().getSyncBatchSize() );

            return Flux.from( new SyncPublisher( syncStream ) );
        } );
    }

    private Map<String,AnyValue> recordAsMap( FabricQuery.RemoteQuery query, Record inputRecord )
    {
        return Records.asMap( inputRecord, seqAsJavaList( query.columns().incoming() ) );
    }

    private FabricConfig.Graph evalFrom( FabricQuery.RemoteQuery query, MapValue params, Map<String,AnyValue> record )
    {
        Catalog.Graph graph = fromEvaluation.evaluate( query.from(), params, record );
        if ( graph instanceof Catalog.RemoteGraph )
        {
            return ((Catalog.RemoteGraph) graph).graph();
        }
        else
        {
            throw notImplemented( "Graph was not a ShardGraph", graph.toString() );
        }
    }

    private MapValue addImportParams( MapValue params, Map<String,AnyValue> record, Map<String,String> bindings )
    {
        MapValueBuilder builder = new MapValueBuilder( params.size() + bindings.size() );
        params.foreach( builder::add );
        bindings.forEach( ( var, par ) -> builder.add( par, validateValue( record.get( var ) ) ) );
        return builder.build();
    }

    private AnyValue validateValue( AnyValue value )
    {
        if ( value instanceof VirtualNodeValue )
        {
            throw new FabricException( Status.Statement.TypeError, "Importing node values in remote subqueries is currently not supported" );
        }
        else if ( value instanceof VirtualRelationshipValue )
        {
            throw new FabricException( Status.Statement.TypeError, "Importing relationship values in remote subqueries is currently not supported" );
        }
        else if ( value instanceof PathValue )
        {
            throw new FabricException( Status.Statement.TypeError, "Importing path values in remote subqueries is currently not supported" );
        }
        else
        {
            return value;
        }
    }

    private UnsupportedOperationException notImplemented( String msg, FabricQuery query )
    {
        return notImplemented( msg, query.toString() );
    }

    private UnsupportedOperationException notImplemented( String msg, String info )
    {
        return new UnsupportedOperationException( msg + ": " + info );
    }

    private Plan.QueryTask.QueryMode getMode( FabricQuery.RemoteQuery query )
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
