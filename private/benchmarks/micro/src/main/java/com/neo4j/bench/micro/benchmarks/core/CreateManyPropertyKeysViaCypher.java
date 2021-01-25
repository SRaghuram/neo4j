/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.Main;
import com.neo4j.bench.micro.benchmarks.TxBatch;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@BenchmarkEnabled( true )
public class CreateManyPropertyKeysViaCypher extends AbstractCoreBenchmark
{
    private static final int MIN_INVOCATIONS_PER_TX = 5;
    private static final int PROPERTY_KEYS_PER_TX = 10000;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000"},
            base = {"100"} )
    @Param( {} )
    public int propsPerMap;

    @ParamValues(
            allowed = {"standard", "high_limit"},
            base = {"standard"} )
    @Param( {} )
    public String format;

    @ParamValues(
            allowed = {"off_heap", "on_heap", "default"},
            base = {"default"} )
    @Param( {} )
    public String txMemory;

    @Override
    public String description()
    {
        return "Tests performance of creating multiple new property keys using a cypher map parameter.\n " +
               "As tokens cannot be deleted, this benchmark cannot achieve fixed state. Therefore it\n" +
               "executes for a fixed number of executions instead of for a fixed time.\n";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .withNodeCount( 1 )
                .withPropertyOrder( Order.ORDERED )
                .withNeo4jConfig( Neo4jConfigBuilder.empty().withSetting( record_format, format ).build() )
                .isReusableStore( false )
                .build();
    }

    @State( Scope.Thread )
    public static class WriteTxState
    {
        TxBatch txBatch;
        Node node;
        String keyBase;
        int propTokenCount;
        int propsPerMap;
        Map<String,Object> params;
        Map<String,Object> prop;

        GraphDatabaseService db;

        @SuppressWarnings( "unchecked" )
        @Setup
        public void setUp( ThreadParams threadParams, CreateManyPropertyKeysViaCypher benchmarkState )
        {
            int thread = threadParams.getThreadIndex();
            keyBase = "Thread(" + thread + ")";
            propTokenCount = 0;
            propsPerMap = benchmarkState.propsPerMap;
            db = benchmarkState.db();
            // This setup means that we will always commit the same number of property keys per tx,
            // regardless of the number of properties written in every invocation. The only exception
            // is if `propsPerMap` is close to or bigger that than the wanted property keys per tx. In
            // that case we make sure to to some minimum number of invocations per tx, to not get too high
            // overhead from the commit process.
            txBatch = new TxBatch( db, Math.max( MIN_INVOCATIONS_PER_TX, PROPERTY_KEYS_PER_TX / propsPerMap ) );
            params = new HashMap<>( 2 );
            prop = new HashMap<>( 1 );
            params.put( "prop", prop );
        }

        void advance()
        {
            if ( txBatch.advance() )
            {
                if ( node != null )
                {
                    txBatch.transaction().getNodeById( node.getId() ).delete();
                }
                node = txBatch.transaction().createNode();
            }
        }

        @TearDown
        public void tearDown()
        {
            txBatch.close();
        }

        String nextPropertyToken()
        {
            return keyBase + (propTokenCount++);
        }

        Map<String,Object> params()
        {
            params.put( "id", node.getId() );
            prop.clear();
            for ( int i = 0; i < propsPerMap; i++ )
            {
                prop.put( nextPropertyToken(), 42 );
            }
            return params;
        }
    }

    /**
     * This is a special setup, which is required because every invocation increases the cost of later invocations.
     * <p>
     * The initial warm-up invocations are fast, so we require more of them to make sure JIT has happened. We then
     * perform relatively few measurement invocations, partly because they are now starting to get quite slow, and
     * partly because we don't want the work performed by the first and last measurement invocation to differ too
     * much.
     */
    @Benchmark
    @Warmup( iterations = 5, batchSize = 30 ) // do enough work to warm-up and have 5 * 30 * propsPerMap property keys
    @Measurement( iterations = 5, batchSize = 1 ) // do only a few single-shot measurements so they execute of roughly the same state
    @BenchmarkMode( Mode.SingleShotTime )
    public void setPropertiesFromMap( WriteTxState writeTxState )
    {
        writeTxState.advance();
        Map<String,Object> params = writeTxState.params();
        writeTxState.txBatch.transaction().execute( "MATCH (n) WHERE id(n)=$id SET n += $prop", params );
    }

    public static void main( String... methods ) throws Exception
    {
        Main.run( CreateManyPropertyKeysViaCypher.class, methods );
    }
}
