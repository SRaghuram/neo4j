/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.planning.FabricPlan;
import com.neo4j.fabric.stream.FabricExecutionStatementResult;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.Summary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.graphdb.QueryExecutionType;

class FabricExecutionStatementResultImpl implements FabricExecutionStatementResult
{
    private final StatementResult statementResult;
    private final Mono<QueryExecutionType> queryExecutionType;

    FabricExecutionStatementResultImpl( StatementResult statementResult, FabricPlan plan, AccessMode accessMode )
    {
        this.statementResult = statementResult;
        this.queryExecutionType = Mono.just(queryExecutionType( plan, accessMode ));
    }

    private static QueryExecutionType queryExecutionType( FabricPlan plan, AccessMode accessMode )
    {
        if ( plan.executionType() == FabricPlan.EXECUTE() )
        {
            return QueryExecutionType.query( queryType( plan, accessMode ) );
        }
        else if ( plan.executionType() == FabricPlan.EXPLAIN() )
        {
            return QueryExecutionType.explained( queryType( plan, accessMode ) );
        }
        else if ( plan.executionType() == FabricPlan.PROFILE() )
        {
            return QueryExecutionType.profiled( queryType( plan, accessMode ) );
        }
        else
        {
            throw unexpected( "execution type", plan.executionType().toString() );
        }
    }

    private static QueryExecutionType.QueryType queryType( FabricPlan plan, AccessMode accessMode )
    {
        return EffectiveQueryType.effectiveQueryType( accessMode, plan.queryType());
    }

    private static IllegalArgumentException unexpected( String type, String got )
    {
        return new IllegalArgumentException( "Unexpected " + type + ": " + got );
    }

    @Override
    public Mono<QueryExecutionType> queryExecutionType()
    {
        return queryExecutionType;
    }

    @Override
    public Flux<String> columns()
    {
        return statementResult.columns();
    }

    @Override
    public Flux<Record> records()
    {
        return statementResult.records();
    }

    @Override
    public Mono<Summary> summary()
    {
        return statementResult.summary();
    }
}
