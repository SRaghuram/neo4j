/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.eval.Catalog;
import com.neo4j.fabric.eval.CatalogManager;
import com.neo4j.fabric.eval.UseEvaluation;
import com.neo4j.fabric.planning.FabricPlan;
import com.neo4j.fabric.planning.FabricPlanner;
import com.neo4j.fabric.planning.FabricQuery;
import com.neo4j.fabric.planning.Fragment;
import com.neo4j.fabric.planning.QueryType;
import com.neo4j.fabric.stream.CompletionDelegatingOperator;
import com.neo4j.fabric.stream.Prefetcher;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Records;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.StatementResults;
import com.neo4j.fabric.stream.summary.MergedSummary;
import com.neo4j.fabric.stream.summary.Summary;
import com.neo4j.fabric.transaction.FabricTransaction;
import com.neo4j.fabric.transaction.TransactionMode;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.cypher.internal.CypherQueryObfuscator;
import org.neo4j.cypher.internal.FullyParsedQuery;
import org.neo4j.cypher.internal.ast.GraphSelection;
import org.neo4j.exceptions.InvalidSemanticsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static scala.collection.JavaConverters.asJavaIterable;
import static scala.collection.JavaConverters.mapAsJavaMap;
import static scala.collection.JavaConverters.seqAsJavaList;

public class FabricExecutor
{
    private final FabricConfig.DataStream dataStreamConfig;
    private final FabricPlanner planner;
    private final UseEvaluation useEvaluation;
    private final CatalogManager catalogManager;
    private final Log log;
    private final FabricQueryMonitoring queryMonitoring;
    private final Executor fabricWorkerExecutor;

    public FabricExecutor( FabricConfig config, FabricPlanner planner, UseEvaluation useEvaluation, CatalogManager catalogManager,
                           LogProvider internalLog,
                           FabricQueryMonitoring queryMonitoring, Executor fabricWorkerExecutor )
    {
        this.dataStreamConfig = config.getDataStream();
        this.planner = planner;
        this.useEvaluation = useEvaluation;
        this.catalogManager = catalogManager;
        this.log = internalLog.getLog( getClass() );
        this.queryMonitoring = queryMonitoring;
        this.fabricWorkerExecutor = fabricWorkerExecutor;
    }

    public StatementResult run( FabricTransaction fabricTransaction, String queryString, MapValue queryParams )
    {
        Thread thread = Thread.currentThread();
        FabricQueryMonitoring.QueryMonitor queryMonitor =
                queryMonitoring.queryMonitor( fabricTransaction.getTransactionInfo(), queryString, queryParams, thread );
        queryMonitor.start();

        String defaultGraphName = fabricTransaction.getTransactionInfo().getDatabaseName();
        FabricPlanner.PlannerInstance plannerInstance = planner.instance( queryString, queryParams, defaultGraphName );
        UseEvaluation.Instance useEvaluator = useEvaluation.instance( queryString );
        FabricPlan plan = plannerInstance.plan();
        Fragment query = plan.query();

        queryMonitor.getMonitoredQuery().onObfuscatorReady( CypherQueryObfuscator.apply( plan.obfuscationMetadata() ) );

        AccessMode accessMode = fabricTransaction.getTransactionInfo().getAccessMode();

        if ( plan.debugOptions().logPlan() )
        {
            log.debug( String.format( "Fabric plan: %s", Fragment.pretty().asString( query ) ) );
        }
        return fabricTransaction.execute(
                ctx ->
                {
                    FabricStatementExecution execution;
                    if ( plan.debugOptions().logRecords() )
                    {
                        execution = new FabricLoggingStatementExecution(
                                plan, plannerInstance, useEvaluator, queryParams, accessMode, ctx, log, queryMonitor, dataStreamConfig
                        );
                    }
                    else
                    {
                        execution = new FabricStatementExecution(
                                plan, plannerInstance, useEvaluator, queryParams, accessMode, ctx, queryMonitor, dataStreamConfig
                        );
                    }
                    return execution.run();
                } );
    }

    private class FabricStatementExecution
    {
        private final FabricPlan plan;
        private final FabricPlanner.PlannerInstance plannerInstance;
        private final UseEvaluation.Instance useEvaluator;
        private final MapValue queryParams;
        private final FabricTransaction.FabricExecutionContext ctx;
        private final MergedSummary mergedSummary;
        private final FabricQueryMonitoring.QueryMonitor queryMonitor;
        private final Prefetcher prefetcher;
        private final AccessMode accessMode;

        FabricStatementExecution(
                FabricPlan plan,
                FabricPlanner.PlannerInstance plannerInstance,
                UseEvaluation.Instance useEvaluator,
                MapValue queryParams,
                AccessMode accessMode,
                FabricTransaction.FabricExecutionContext ctx,
                FabricQueryMonitoring.QueryMonitor queryMonitor,
                FabricConfig.DataStream dataStreamConfig )
        {
            this.plan = plan;
            this.plannerInstance = plannerInstance;
            this.useEvaluator = useEvaluator;
            this.queryParams = queryParams;
            this.ctx = ctx;
            this.mergedSummary = new MergedSummary( plan, accessMode );
            this.queryMonitor = queryMonitor;
            this.prefetcher = new Prefetcher( dataStreamConfig );
            this.accessMode = accessMode;
        }

        StatementResult run()
        {
            queryMonitor.startExecution();
            var query = plan.query();
            Flux<String> columns = Flux.fromIterable( asJavaIterable( query.outputColumns() ) );
            Mono<Summary> summary = Mono.just( mergedSummary );

            Flux<Record> records;
            if ( plan.executionType() == FabricPlan.EXPLAIN() )
            {
                records = Flux.empty();
            }
            else
            {
                records = run( query, null );
            }
            return StatementResults.create(
                    columns,
                    records.doOnComplete( queryMonitor::endSuccess )
                           .doOnError( queryMonitor::endFailure ),
                    summary
            );
        }

        Flux<Record> run( Fragment fragment, Record argument )
        {

            if ( fragment instanceof Fragment.Init )
            {
                return runInit( (Fragment.Init) fragment, argument );
            }
            else if ( fragment instanceof Fragment.Apply )
            {
                return runApply( (Fragment.Apply) fragment, argument );
            }
            else if ( fragment instanceof Fragment.Union )
            {
                return runUnion( (Fragment.Union) fragment, argument );
            }
            else if ( fragment instanceof Fragment.Exec )
            {
                return runExec( (Fragment.Exec) fragment, argument );
            }
            else
            {
                throw notImplemented( "Invalid query fragment", fragment );
            }
        }

        Flux<Record> runInit( Fragment.Init input, Record argument )
        {
            return Flux.just( Records.empty() );
        }

        Flux<Record> runApply( Fragment.Apply apply, Record argument )
        {
            Flux<Record> input = run( apply.input(), argument );

            return input.flatMap(
                    record -> run( apply.inner(), record ).map( outputRecord -> Records.join( record, outputRecord ) ),
                    dataStreamConfig.getConcurrency(), 1
            );
        }

        Flux<Record> runUnion( Fragment.Union union, Record argument )
        {
            Flux<Record> lhs = run( union.lhs(), argument );
            Flux<Record> rhs = run( union.rhs(), argument );
            Flux<Record> merged = Flux.merge( lhs, rhs );
            if ( union.distinct() )
            {
                return merged.distinct();
            }
            else
            {
                return merged;
            }
        }

        Flux<Record> runExec( Fragment.Exec fragment, Record argument )
        {
            Map<String,AnyValue> argumentValues = argumentValues( fragment, argument );
            MapValue parameters = addParamsFromRecord( queryParams, argumentValues, mapAsJavaMap( fragment.parameters() ) );

            Catalog.Graph graph = evalUse( fragment.use().graphSelection(), argumentValues );
            var transactionMode = getTransactionMode( fragment.queryType(), graph.toString() );
            Location location = catalogManager.locationOf( graph, transactionMode.requiresWrite() );
            if ( location instanceof Location.Local )
            {
                Location.Local local = (Location.Local) location;
                Flux<Record> input = run( fragment.input(), argument );
                if ( fragment.executable() )
                {
                    FabricQuery.LocalQuery localQuery = plannerInstance.asLocal( fragment );
                    return runLocalQueryAt( local, transactionMode, localQuery.query(), parameters, input );
                }
                else
                {
                    return input;
                }
            }
            else if ( location instanceof Location.Remote )
            {
                Location.Remote remote = (Location.Remote) location;
                FabricQuery.RemoteQuery remoteQuery = plannerInstance.asRemote( fragment );
                return runRemoteQueryAt( remote, transactionMode, remoteQuery.query(), parameters );
            }
            else
            {
                throw notImplemented( "Invalid graph location", location );
            }
        }

        Flux<Record> runLocalQueryAt( Location.Local location, TransactionMode transactionMode, FullyParsedQuery query, MapValue parameters,
                                      Flux<Record> input )
        {
            return ctx.getLocal()
                      .run( location, transactionMode, queryMonitor.getMonitoredQuery(), query, parameters, input )
                      .records();
        }

        Flux<Record> runRemoteQueryAt( Location.Remote location, TransactionMode transactionMode, String queryString, MapValue parameters )
        {
            Flux<Record> records = ctx.getRemote()
                                      .run( location, queryString, transactionMode, parameters )
                                      .flatMapMany( statementResult -> statementResult.records()
                                                                                      .doOnComplete( () -> statementResult.summary().subscribe(
                                                                                              this::updateSummary ) ) );
            // 'onComplete' signal coming from an inner stream might cause more data being requested from an upstream operator
            // and the request will be done using the thread that invoked 'onComplete'.
            // Since 'onComplete' is invoked by driver IO thread ( Netty event loop ), this might cause the driver thread to block
            // or perform a computationally intensive operation in an upstream operator if the upstream operator is Cypher local execution
            // that produces records directly in 'request' call.
            Flux<Record> recordsWithCompletionDelegation = new CompletionDelegatingOperator( records, fabricWorkerExecutor );
            return prefetcher.addPrefetch( recordsWithCompletionDelegation );
        }

        private Map<String,AnyValue> argumentValues( Fragment fragment, Record argument )
        {
            if ( argument == null )
            {
                return Map.of();
            }
            else
            {
                return Records.asMap( argument, seqAsJavaList( fragment.argumentColumns() ) );
            }
        }

        private Catalog.Graph evalUse( GraphSelection selection, Map<String,AnyValue> record )
        {
            return useEvaluator.evaluate( selection, queryParams, record );
        }

        private MapValue addParamsFromRecord( MapValue params, Map<String,AnyValue> record, Map<String,String> bindings )
        {
            int resultSize = params.size() + bindings.size();
            if ( resultSize == 0 )
            {
                return VirtualValues.EMPTY_MAP;
            }
            MapValueBuilder builder = new MapValueBuilder( resultSize );
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

        private RuntimeException notImplemented( String msg, Object object )
        {
            return notImplemented( msg, object.toString() );
        }

        private RuntimeException notImplemented( String msg, String info )
        {
            return new InvalidSemanticsException( msg + ": " + info );
        }

        private TransactionMode getTransactionMode( QueryType queryType, String graph )
        {
            var queryMode = EffectiveQueryType.effectiveAccessMode( accessMode, queryType );

            if ( accessMode == AccessMode.WRITE )
            {
                if ( queryMode == AccessMode.WRITE )
                {
                    return TransactionMode.DEFINITELY_WRITE;
                }
                else
                {
                    return TransactionMode.MAYBE_WRITE;
                }
            }
            else
            {
                if ( queryMode == AccessMode.WRITE )
                {
                    throw new FabricException( Status.Fabric.AccessMode, "Writing in read access mode not allowed. Attempted write to %s", graph );
                }
                else
                {
                    return TransactionMode.DEFINITELY_READ;
                }
            }
        }
    }

    private class FabricLoggingStatementExecution extends FabricStatementExecution
    {
        private final AtomicInteger step;
        private final Log log;

        FabricLoggingStatementExecution(
                FabricPlan plan,
                FabricPlanner.PlannerInstance plannerInstance,
                UseEvaluation.Instance useEvaluator, MapValue params,
                AccessMode accessMode,
                FabricTransaction.FabricExecutionContext ctx,
                Log log,
                FabricQueryMonitoring.QueryMonitor queryMonitor,
                FabricConfig.DataStream dataStreamConfig )
        {
            super( plan, plannerInstance, useEvaluator, params, accessMode, ctx, queryMonitor, dataStreamConfig );
            this.step = new AtomicInteger( 0 );
            this.log = log;
        }

        @Override
        Flux<Record> runLocalQueryAt( Location.Local location, TransactionMode transactionMode, FullyParsedQuery query, MapValue parameters,
                                      Flux<Record> input )
        {
            String id = executionId();
            trace( id, "local " + location.getGraphId(), compact( query.description() ) );
            return traceRecords( id, super.runLocalQueryAt( location, transactionMode, query, parameters, input ) );
        }

        @Override
        Flux<Record> runRemoteQueryAt( Location.Remote location, TransactionMode transactionMode, String queryString, MapValue parameters )
        {
            String id = executionId();
            trace( id, "remote " + location.getGraphId(), compact( queryString ) );
            return traceRecords( id, super.runRemoteQueryAt( location, transactionMode, queryString, parameters ) );
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
