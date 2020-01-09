/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.eval.Catalog;
import com.neo4j.fabric.eval.UseEvaluation;
import com.neo4j.fabric.planning.FabricPlan;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.planning.FabricQuery;
import com.neo4j.fabric.planning.QueryType;
import com.neo4j.fabric.stream.Prefetcher;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.stream.summary.MergedSummary;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransaction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.cypher.internal.v4_0.ast.UseGraph;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
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
    private final FabricConfig.DataStream dataStreamConfig;
    private final FabricPlanner planner;
    private final UseEvaluation useEvaluation;
    private final Log log;
    private final FabricQueryMonitoring queryMonitoring;

    public FabricExecutor( FabricConfig config, FabricPlanner planner, UseEvaluation useEvaluation, LogProvider internalLog,
            FabricQueryMonitoring queryMonitoring )
    {
        this.dataStreamConfig = config.getDataStream();
        this.planner = planner;
        this.useEvaluation = useEvaluation;
        this.log = internalLog.getLog( getClass() );
        this.queryMonitoring = queryMonitoring;
    }

    public StatementResult run( FabricTransaction fabricTransaction, String statement, MapValue params )
    {
        Thread thread = Thread.currentThread();
        FabricQueryMonitoring.QueryMonitor queryMonitor = queryMonitoring.queryMonitor( fabricTransaction.getTransactionInfo(), statement, params, thread );
        queryMonitor.start();

        FabricPlan plan = planner.plan( statement, params );

        AccessMode accessMode = fabricTransaction.getTransactionInfo().getAccessMode();

        if ( plan.debugOptions().logPlan() )
        {
            log.debug( String.format( "Fabric plan: %s", FabricQuery.pretty().asString( plan.query() ) ) );
        }
        return fabricTransaction.execute( ctx ->
        {
            FabricStatementExecution execution;
            if ( plan.debugOptions().logRecords() )
            {
                execution = new FabricLoggingStatementExecution( statement, plan, params, accessMode, ctx, log, queryMonitor, dataStreamConfig );
            }
            else
            {
                execution = new FabricStatementExecution( statement, plan, params, accessMode, ctx, queryMonitor, dataStreamConfig );
            }
            return execution.run();
        } );
    }

    class FabricStatementExecution
    {
        private final String originalStatement;
        private final FabricPlan plan;
        private final MapValue params;
        private final FabricTransaction.FabricExecutionContext ctx;
        private final MergedSummary mergedSummary;
        private final FabricQueryMonitoring.QueryMonitor queryMonitor;
        private final Prefetcher prefetcher;

        FabricStatementExecution( String originalStatement, FabricPlan plan, MapValue params, AccessMode accessMode,
                                  FabricTransaction.FabricExecutionContext ctx,
                                  FabricQueryMonitoring.QueryMonitor queryMonitor, FabricConfig.DataStream dataStreamConfig )
        {
            this.originalStatement = originalStatement;
            this.plan = plan;
            this.params = params;
            this.ctx = ctx;
            this.mergedSummary = new MergedSummary( plan, accessMode );
            this.queryMonitor = queryMonitor;
            this.prefetcher = new Prefetcher( dataStreamConfig );
        }

        StatementResult run()
        {
            queryMonitor.startExecution();
            var query = plan.query();
            Flux<Record> unit = Flux.just( Records.empty() );
            Flux<String> columns = Flux.fromIterable( asJavaIterable( query.columns().output() ) );
            Mono<Summary> summary = Mono.just( mergedSummary );

            Flux<Record> records;
            if ( plan.executionType() == FabricPlan.EXPLAIN() )
            {
                records = Flux.empty();
            }
            else
            {
                records = run( query, unit );
            }
            return StatementResults.create(
                    columns,
                    records.doOnComplete( queryMonitor::endSuccess )
                            .doOnError( queryMonitor::endFailure ),
                    summary
            );
        }

        Flux<Record> run( FabricQuery query, Flux<Record> input )
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

        Flux<Record> runChainedQuery( FabricQuery.ChainedQuery query, Flux<Record> input )
        {
            Flux<Record> previous = input;
            for ( FabricQuery q : asJavaIterable( query.queries() ) )
            {
                previous = run( q, previous );
            }

            return previous;
        }

        Flux<Record> runUnionQuery( FabricQuery.UnionQuery query, Flux<Record> input )
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

        Flux<Record> runDirectQuery( FabricQuery.Direct query, Flux<Record> input )
        {
            return run( query.query(), input );
        }

        Flux<Record> runApplyQuery( FabricQuery.Apply query, Flux<Record> input )
        {
            return input.flatMap(
                    inputRecord -> run( query.query(), Flux.just( inputRecord ) ).map( outputRecord -> Records.join( inputRecord, outputRecord ) ),
                    dataStreamConfig.getConcurrency(), 1
            );
        }

        Flux<Record> runLocalQuery( FabricQuery.LocalQuery query, Flux<Record> input )
        {
            return ctx.getLocal().run( queryMonitor.getMonitoredQuery(), query.query(), params, input ).records();
        }

        Flux<Record> runRemoteQuery( FabricQuery.RemoteQuery query, Flux<Record> input )
        {
            String queryString = query.queryString();
            QueryType queryType = query.queryType();
            return input.flatMap( inputRecord ->
            {
                Map<String,AnyValue> recordValues = recordAsMap( query, inputRecord );
                FabricConfig.Graph graph = evalUse( query.use(), recordValues );
                MapValue parameters = addImportParams( recordValues, mapAsJavaMap( query.parameters() ) );
                return runRemoteQueryAt( graph, queryString, queryType, parameters );
            }, dataStreamConfig.getConcurrency(), 1 );
        }

        Flux<Record> runRemoteQueryAt( FabricConfig.Graph graph, String queryString, QueryType queryType, MapValue parameters )
        {
            Flux<Record> records =  ctx.getRemote().run( graph, queryString, queryType, parameters )
                    .flatMapMany( statementResult -> statementResult.records()
                            .doOnComplete( () -> statementResult.summary().subscribe( this::updateSummary ) ) );
            return prefetcher.addPrefetch( records );
        }

        private Map<String,AnyValue> recordAsMap( FabricQuery.RemoteQuery query, Record inputRecord )
        {
            return Records.asMap( inputRecord, seqAsJavaList( query.columns().incoming() ) );
        }

        private FabricConfig.Graph evalUse( UseGraph use, Map<String,AnyValue> record )
        {
            Catalog.Graph graph = useEvaluation.evaluate( originalStatement, use, params, record );
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

        private void updateSummary( Summary summary )
        {
            if ( summary != null )
            {
                this.mergedSummary.add( summary.getQueryStatistics() );
                this.mergedSummary.add( summary.getNotifications() );
            }
        }

        private RuntimeException notImplemented( String msg, FabricQuery query )
        {
            return notImplemented( msg, query.toString() );
        }

        private RuntimeException notImplemented( String msg, String info )
        {
            return new InvalidSemanticsException( msg + ": " + info );
        }

    }

    class FabricLoggingStatementExecution extends FabricStatementExecution
    {
        private final AtomicInteger step;
        private final Log log;

        FabricLoggingStatementExecution( String originalStatement, FabricPlan plan, MapValue params, AccessMode accessMode,
                                         FabricTransaction.FabricExecutionContext ctx, Log log,
                                         FabricQueryMonitoring.QueryMonitor queryMonitor, FabricConfig.DataStream dataStreamConfig )
        {
            super( originalStatement, plan, params, accessMode, ctx, queryMonitor, dataStreamConfig );
            this.step = new AtomicInteger( 0 );
            this.log = log;
        }

        @Override
        Flux<Record> runLocalQuery( FabricQuery.LocalQuery query, Flux<Record> input )
        {
            String id = executionId();
            trace( id, "local", compact( query.query().description() ) );
            return traceRecords( id, super.runLocalQuery( query, input ) );
        }

        @Override
        Flux<Record> runRemoteQueryAt( FabricConfig.Graph graph, String queryString, QueryType queryType, MapValue parameters )
        {
            String id = executionId();
            trace( id, "remote " + graph.getId(), compact( queryString ) );
            return traceRecords( id, super.runRemoteQueryAt( graph, queryString, queryType, parameters ) );
        }

        private String compact( String in )
        {
            return in.replaceAll( "\\r?\\n", " " ).replaceAll( "\\s+", " " );
        }

        private Flux<Record> traceRecords( String id, Flux<Record> flux )
        {
            return flux.doOnNext( record ->
            {
                String rec = IntStream.range( 0, record.size() )
                        .mapToObj( i -> record.getValue( i ).toString() )
                        .collect( Collectors.joining( ", ", "[", "]" ) );
                trace( id, "output", rec );
            } );
        }

        private void trace( String id, String event, String data )
        {
            log.debug( String.format( "%s: %s: %s", id, event, data ) );
        }

        private String executionId()
        {
            String stmtId = idString( this.hashCode() );
            String step = idString( this.step.getAndIncrement() );
            return String.format( "%s/%s", stmtId, step );
        }

        private String idString( int code )
        {
            return String.format( "%08X", code );
        }
    }
}
