package com.neo4j.bench.micro.benchmarks.bolt;

import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
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

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltFactoryImpl;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

import static org.neo4j.helpers.collection.MapUtil.map;

@BenchmarkEnabled( true )
@OutputTimeUnit( TimeUnit.MICROSECONDS )
public class BoltPropertySerialization extends AbstractBoltBenchmark
{
    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR, BYTE_ARR},
            base = {LNG, STR_SML, STR_BIG, BYTE_ARR} )
    @Param( {} )
    public String BoltPropertySerialization_type;

    @ParamValues(
            allowed = {"compiled", "interpreted"},
            base = {"compiled", "interpreted"} )
    @Param( {} )
    public String BoltPropertySerialization_runtime;

    @ParamValues(
            allowed = {"1", "10", "100", "1000", "10000", "100000", "1000000"},
            base = {"1", "100000"} )
    @Param( {} )
    public int BoltPropertySerialization_nodeCount;

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
                .withNodeCount( BoltPropertySerialization_nodeCount )
                .withNodeProperties(
                        new PropertyDefinition( "prop", randPropertyFor( BoltPropertySerialization_type ).value() ) )
                .isReusableStore( true )
                .build();
    }

    @State( Scope.Thread )
    public static class BoltMachine
    {
        private BoltFactoryImpl boltFactory;
        private BoltStateMachine machine;
        private String query;
        private DummyBoltResultHandler handler = new DummyBoltResultHandler();

        @Setup
        public void setup( BoltPropertySerialization state ) throws Throwable
        {
            query = String.format( "CYPHER runtime=%s MATCH (n) RETURN n.prop",
                    state.BoltPropertySerialization_runtime );
            boltFactory = boltFactory( (GraphDatabaseAPI) state.db() );
            boltFactory.start();
            machine = boltFactory.newMachine( BOLT_CHANNEL, Clock.systemUTC() );
            machine.init( USER_AGENT, map(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j" ), RESPONSE_HANDLER );
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
                boltFactory.stop();
                boltFactory = null;
            }
        }

        byte[] getPropertyForAllNodes() throws BoltConnectionFatality
        {
            handler.reset();
            machine.run( query, VirtualValues.EMPTY_MAP, handler );
            machine.pullAll( handler );
            return handler.result();
        }
    }

    @Benchmark
    @BenchmarkMode( {Mode.SampleTime} )
    public byte[] serializeProperty( BoltMachine machine ) throws BoltConnectionFatality
    {
        return machine.getPropertyForAllNodes();
    }

    public static void main( String... methods ) throws Exception
    {
        run( BoltPropertySerialization.class, methods );
    }
}
