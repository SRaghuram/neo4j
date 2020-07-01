/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.bolt;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.PropertyDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.BYTE_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.longValue;

@BenchmarkEnabled( true )
@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class BoltPropertySerialization extends AbstractBoltBenchmark
{
    private static final MapValue DEFAULT_META_VALUE =
            VirtualValues.map( new String[]{"n"}, new AnyValue[]{longValue( -1L )} );
    static final PullMessage META_MSG = pullMessage();

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR, BYTE_ARR},
            base = {LNG, STR_SML, STR_BIG, POINT, BYTE_ARR} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"interpreted"},
            base = {"interpreted"} )
    @Param( {} )
    public String runtime;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000", "100000", "1000000"},
            base = {"1", "100000"} )
    @Param( {} )
    public int nodeCount;

    @Override
    public String description()
    {
        return "Tests performance of property serialization with cypher over bolt.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        return new DataGeneratorConfigBuilder()
                .withNodeCount( nodeCount )
                .withNodeProperties(
                        new PropertyDefinition( "prop", randPropertyFor( type ).value() ) )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class BoltMachine
    {
        private BoltStateMachineFactory boltFactory;
        private BoltStateMachine machine;
        private String query;
        private DummyBoltResultHandler handler = new DummyBoltResultHandler();

        @Setup
        public void setup( BoltPropertySerialization state ) throws Throwable
        {
            query = String.format( "CYPHER runtime=%s MATCH (n) RETURN n.prop",
                                   state.runtime );
            boltFactory = boltFactory( (GraphDatabaseAPI) state.db() );
            machine = boltFactory.newStateMachine( BOLT_VERSION, BOLT_CHANNEL );
            var hello = new HelloMessage( map(
                    "user_agent", USER_AGENT,
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j" ) );
            machine.process( hello, RESPONSE_HANDLER );
        }

        @TearDown
        public void tearDown() throws Throwable
        {
            if ( machine != null )
            {
                machine.close();
                machine = null;
            }
            if ( boltFactory != null )
            {
                boltFactory = null;
            }
        }

        byte[] getPropertyForAllNodes() throws BoltConnectionFatality, BoltIOException
        {
            handler.reset();
            machine.process( new RunMessage( query, VirtualValues.EMPTY_MAP ), handler );
            machine.process( META_MSG, handler );
            return handler.result();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeProperty( BoltMachine machine ) throws BoltConnectionFatality, BoltIOException
    {
        return machine.getPropertyForAllNodes();
    }

    public static void main( String... methods ) throws Exception
    {
        run( BoltPropertySerialization.class, methods );
    }

    private static PullMessage pullMessage()
    {
        try
        {
            return new PullMessage( DEFAULT_META_VALUE );
        }
        catch ( BoltIOException e )
        {
            throw new AssertionError( "failed to intitalize" );
        }
    }
}
