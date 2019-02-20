/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.bolt;

import com.neo4j.bench.micro.benchmarks.RNGState;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.RelationshipDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFun;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.ThreadParams;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static com.neo4j.bench.micro.Main.run;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@BenchmarkEnabled( true )
@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class BoltValueSerialization extends AbstractBoltBenchmark
{
    private static final  RelationshipDefinition RELATIONSHIP_DEFINITION = new RelationshipDefinition(
            RelationshipType.withName( "REL" ), 1 );

    @ParamValues(
            allowed = {"compiled", "interpreted"},
            base = {"compiled", "interpreted"} )
    @Param( {} )
    public String BoltValueSerialization_runtime;

    @Override
    public String description()
    {
        return "Tests performance of serializing graph entities over bolt.";
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
                .withNodeCount( 1 )
                .withNodeProperties( properties() )
                .withOutRelationships( RELATIONSHIP_DEFINITION )
                .withRelationshipProperties( properties() )
                .isReusableStore( true )
                .build();
    }

    private PropertyDefinition[] properties()
    {
        return new PropertyDefinition[]{
                new PropertyDefinition( "prop1", randPropertyFor( STR_SML ).value() ),
                new PropertyDefinition( "prop2", randPropertyFor( LNG ).value() ),
                new PropertyDefinition( "prop3", randPropertyFor( DBL ).value() ),
        };
    }

    @State( Scope.Thread )
    public static class BoltMachine
    {
        private BoltStateMachineFactory boltFactory;
        private BoltStateMachine machine;
        private String prefix;
        private DummyBoltResultHandler handler = new DummyBoltResultHandler();
        private SplittableRandom random;
        private MapValue listParam;
        private MapValue mapParam;
        private MapValue stringParam;

        @Setup
        public void setup( BoltValueSerialization state, ThreadParams threadParams ) throws Throwable
        {
            random = RNGState.newRandom( threadParams );
            prefix = String.format( "CYPHER runtime=%s ", state.BoltValueSerialization_runtime );
            boltFactory = boltFactory( (GraphDatabaseAPI) state.db() );
            machine = boltFactory.newStateMachine( BOLT_VERSION, BOLT_CHANNEL );
            InitMessage init = new InitMessage( USER_AGENT, map(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j" ) );
            machine.process( init, RESPONSE_HANDLER );
            stringParam = VirtualValues.map( new String[]{"p"}, new AnyValue[]{
                    Values.utf8Value( ((String) randPropertyFor( STR_BIG ).value().create().next( random )).getBytes( StandardCharsets.UTF_8 ) )} );
            ValueGeneratorFun stringFun = randPropertyFor( STR_SML ).value().create();
            ValueGeneratorFun longFun = randPropertyFor( LNG ).value().create();
            ValueGeneratorFun dblFun = randPropertyFor( DBL ).value().create();
            setupList( stringFun, longFun, dblFun );
            setupMap( stringFun, longFun, dblFun );
        }

        private void setupList( ValueGeneratorFun... generators )
        {
            HashMap<String,AnyValue> listParamMap = new HashMap<>();
            int size = 100;
            ArrayList<AnyValue> list = new ArrayList<>( size );
            for ( int i = 0; i < size; i++ )
            {
                list.add( Values.of( generators[i % generators.length].next( random ) ) );
            }
            listParamMap.put( "p", VirtualValues.fromList( list ) );
            listParam = VirtualValues.map( new String[]{"p"}, new AnyValue[]{VirtualValues.fromList( list )} );
        }

        private void setupMap( ValueGeneratorFun... generators )
        {
            int size = 100;
            String[] key = new String[size];
            AnyValue[] values = new AnyValue[size];
            Map<String,AnyValue> map = new HashMap<>( size );
            for ( int i = 0; i < size; i++ )
            {
                key[i] = "k" + i;
                values[i] = Values.of( generators[i % generators.length].next( random ) );
            }
            mapParam = VirtualValues.map( new String[]{"p"}, new AnyValue[]{VirtualValues.map( key, values )} );
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

        byte[] run( String query, MapValue param ) throws BoltConnectionFatality
        {
            handler.reset();
            machine.process( new RunMessage( prefix + query, param ), handler );
            machine.process( PullAllMessage.INSTANCE, handler );
            return handler.result();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeNode( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.run( "MATCH (n) RETURN n", EMPTY_MAP );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeRelationship( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.run( "MATCH ()-[r:REL]->() RETURN r", EMPTY_MAP );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeMap( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.run( "RETURN {p}", machine.mapParam );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeList( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.run( "RETURN {p}", machine.listParam );
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeString( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.run( "RETURN {p}", machine.stringParam );
    }

    public static void main( String... methods ) throws Exception
    {
        run( BoltValueSerialization.class, methods );
    }
}
