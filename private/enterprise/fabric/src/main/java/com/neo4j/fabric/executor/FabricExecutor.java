/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.eval.Catalog;
import com.neo4j.fabric.eval.UseEvaluation;
import com.neo4j.fabric.planner.api.Plan;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.planning.FabricQuery;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.stream.SyncPublisher;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransaction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

import org.neo4j.cypher.internal.v4_0.ast.UseGraph;
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
    private final UseEvaluation useEvaluation;

    public FabricExecutor( FabricConfig config, FabricPlanner planner, UseEvaluation useEvaluation )
    {
        this.config = config;
        this.planner = planner;
        this.useEvaluation = useEvaluation;
    }

    public StatementResult run( FabricTransaction fabricTransaction, String statement, MapValue params )
    {
        FabricQuery query = planner.plan( statement, params );
        return fabricTransaction.execute( ctx ->
        {
            FabricStatementExecution execution = new FabricStatementExecution( query, params, ctx );
            return execution.run();
        } );
    }

    public boolean isPeriodicCommit( String query )
    {
        return planner.isPeriodicCommit( query );
    }

    class FabricStatementExecution
    {
        private final FabricQuery query;
        private final MapValue params;
        private final FabricTransaction.FabricExecutionContext ctx;

        FabricStatementExecution( FabricQuery query, MapValue params, FabricTransaction.FabricExecutionContext ctx )
        {
            this.query = query;
            this.params = params;
            this.ctx = ctx;
        }

        private StatementResult run()
        {
            Flux<Record> unit = Flux.just( Records.empty() );
            Flux<String> columns = Flux.fromIterable( asJavaIterable( query.columns().output() ) );
            Flux<Record> records = run( query, unit );
            Mono<Summary> summary = Mono.empty();
            return StatementResults.create( columns, records, summary );
        }

        private Flux<Record> run( FabricQuery query, Flux<Record> input )
        {
            if ( query instanceof FabricQuery.Direct )
            {
                return runDirectQuery( (FabricQuery.Direct) query, input );
            }
            else if ( query instanceof FabricQuery.Apply )
            {
                return runApplyQuery( (FabricQuery.Apply) query, input );
            }
            else if ( query instanceof FabricQuery.LocalQuery )
            {
                return runLocalQuery( (FabricQuery.LocalQuery) query, input );
            }
            else if ( query instanceof FabricQuery.RemoteQuery )
            {
                return runRemoteQuery( (FabricQuery.RemoteQuery) query, input );
            }
            else if ( query instanceof FabricQuery.ChainedQuery )
            {
                return runChainedQuery( (FabricQuery.ChainedQuery) query, input );
            }
            else if ( query instanceof FabricQuery.UnionQuery )
            {
                return runUnionQuery( (FabricQuery.UnionQuery) query, input );
            }
            else
            {
                throw notImplemented( "Unsupported query", query );
            }
        }

        private Flux<Record> runChainedQuery( FabricQuery.ChainedQuery query, Flux<Record> input )
        {
            Flux<Record> previous = input;
            for ( FabricQuery q : asJavaIterable( query.queries() ) )
            {
                previous = run( q, previous );
            }

            return previous;
        }

        private Flux<Record> runUnionQuery( FabricQuery.UnionQuery query, Flux<Record> input )
        {
            Flux<Record> lhs = run( query.lhs(), input );
            Flux<Record> rhs = run( query.rhs(), input );
            Flux<Record> merged = Flux.merge( lhs, rhs );
            if ( query.distinct() )
            {
                merged = merged.distinct();
            }
            return merged;
        }

        private Flux<Record> runDirectQuery( FabricQuery.Direct query, Flux<Record> input )
        {
            return run( query.query(), input );
        }

        private Flux<Record> runApplyQuery( FabricQuery.Apply query, Flux<Record> input )
        {
            return input.flatMap( inputRecord ->
                    run( query.query(), Flux.just( inputRecord ) )
                            .map( outputRecord ->
                                    Records.join( inputRecord, outputRecord ) ) );
        }

        private Flux<Record> runLocalQuery( FabricQuery.LocalQuery query, Flux<Record> input )
        {
            // Wrap in StatementResult, REMOVE?
            Flux<String> inputCols = Flux.fromIterable( asJavaIterable( query.columns().local() ) );
            StatementResult inputStream = StatementResults.create( inputCols, input, Mono.empty() );
            return ctx.getLocal().run( query.query(), params, inputStream ).records();
        }

        private Flux<Record> runRemoteQuery( FabricQuery.RemoteQuery query, Flux<Record> input )
        {
            String queryString = query.queryString();
            Plan.QueryTask.QueryMode queryMode = getMode( query );
            return input.flatMap( inputRecord ->
            {
                Map<String,AnyValue> recordValues = recordAsMap( query, inputRecord );
                FabricConfig.Graph graph = evalUse( query.use(), recordValues );
                MapValue parameters = addImportParams( recordValues, mapAsJavaMap( query.parameters() ) );

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

        private FabricConfig.Graph evalUse( UseGraph use, Map<String,AnyValue> record )
        {
            Catalog.Graph graph = useEvaluation.evaluate( use, params, record );
            if ( graph instanceof Catalog.RemoteGraph )
            {
                return ((Catalog.RemoteGraph) graph).graph();
            }
            else
            {
                throw notImplemented( "Graph was not a ShardGraph", graph.toString() );
            }
        }

        private MapValue addImportParams( Map<String,AnyValue> record, Map<String,String> bindings )
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
}
