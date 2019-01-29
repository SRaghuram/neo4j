package com.neo4j.bench.micro;

import com.neo4j.bench.client.model.Benchmark.Mode;
import com.neo4j.bench.micro.config.ParameterValue;
import org.junit.Test;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.Lists.newArrayList;
import static com.neo4j.bench.micro.JMHResultUtil.MODE_PARAM;
import static com.neo4j.bench.micro.JMHResultUtil.THREADS_PARAM;
import static com.neo4j.bench.micro.JMHResultUtil.extractParameterValues;
import static com.neo4j.bench.micro.JMHResultUtil.parametersAsMap;
import static com.neo4j.bench.micro.JMHResultUtil.toNameSuffix;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class JMHResultUtilTest
{
    @Test
    public void shouldThrowExceptionWhenDuplicatesInParameterValues()
    {
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k1", "v1" ) ) ) );
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k1", "v2" ) ) ) );
        assertTrue( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v2" ),
                new ParameterValue( "k2", "v2" ) ) ) );

        assertFalse( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v2" ) ) ) );
        assertFalse( throwsExceptionWhenParameterValuesContainsDuplicates( newArrayList(
                new ParameterValue( "k1", "v1" ),
                new ParameterValue( "k2", "v1" ) ) ) );
    }

    private boolean throwsExceptionWhenParameterValuesContainsDuplicates( List<ParameterValue> parameterValues )
    {
        try
        {
            parametersAsMap( parameterValues );
            return false;
        }
        catch ( Throwable e )
        {
            return true;
        }
    }

    @Test
    public void shouldBeDeterministicWhenCreatingBenchmarkNamesFromParamsList()
    {
        long seed = System.currentTimeMillis();
        Random rng = new Random( seed );
        Mode mode = Mode.LATENCY;
        for ( int testRepetition = 1; testRepetition <= 100; testRepetition++ )
        {
            final int valueRange = testRepetition;
            List<ParameterValue> parameterValues = IntStream.range( 0, 100 )
                    .mapToObj( i -> new ParameterValue(
                            Integer.toString( rng.nextInt( valueRange ) ),
                            Integer.toString( rng.nextInt( valueRange ) ) ) )
                    .collect( toList() );
            String name1 = toNameSuffix( parameterValues, mode );
            Collections.shuffle( parameterValues, rng );
            String name2 = toNameSuffix( parameterValues, mode );
            assertThat( format( "Names not equal with Seed: %s%n", seed ),
                    name1, equalTo( name2 ) );
        }
    }

    @Test
    public void shouldExtractJmhParamsAndThreadsCorrectly() throws Exception
    {
        long seed = System.currentTimeMillis();
        Random rng = new Random( seed );

        int threads = 42;
        ParameterValue param1 = new ParameterValue( "param1", "value" );
        ParameterValue param2 = new ParameterValue( "param2", "value" );
        ParameterValue param3 = new ParameterValue( "param3", "value" );
        ParameterValue paramThreads = new ParameterValue( THREADS_PARAM, Integer.toString( threads ) );

        WorkloadParams workloadParams = new WorkloadParams();
        workloadParams.put( "class_" + param1.param(), param1.value(), 1 );
        workloadParams.put( "class_" + param2.param(), param2.value(), 1 );
        workloadParams.put( "class_" + param3.param(), param3.value(), 1 );

        BenchmarkParams benchmarkParams = new BenchmarkParams(
                "doThing", /* benchmark */
                "generatedDoThings", /* generatedTarget */
                false, /* syncIterations */
                Integer.parseInt( paramThreads.value() ),
                new int[]{},/* threadGroups */
                Collections.<String>emptyList(), /* threadGroupLabels */
                1, /* forks */
                1, /* warmupForks */
                new IterationParams(
                        IterationType.WARMUP,
                        1, /* count */
                        TimeValue.seconds( 5 ), /* time */
                        1 /* batchSize */ ),
                new IterationParams(
                        IterationType.MEASUREMENT,
                        1, /* count */
                        TimeValue.seconds( 5 ), /* time */
                        1 /* batchSize */ ),
                org.openjdk.jmh.annotations.Mode.AverageTime, /* mode */
                workloadParams, /* workload params */
                TimeUnit.DAYS, /* time unit */
                1, /* opsPerInvocation */
                "jvm", /* jvm */
                Collections.<String>emptyList(), /* jvmArgs */
                "openjdk", /* vm name*/
                "1.8.0", /* jdk version */
                "1.8.0", /* vm version */
                "1.19", /* jmh version */
                TimeValue.NONE /* timeout */ );

        List<ParameterValue> parameterValues = extractParameterValues( benchmarkParams );

        assertTrue( parameterValues.contains( param1 ) );
        assertTrue( parameterValues.contains( param2 ) );
        assertTrue( parameterValues.contains( param3 ) );
        assertTrue( parameterValues.contains( paramThreads ) );
        assertThat( parameterValues.size(), equalTo( 4 ) );

        Collections.shuffle( parameterValues, rng );
        String nameSuffix = toNameSuffix( parameterValues, Mode.LATENCY );

        assertTrue( nameSuffix.indexOf( param1.param() ) < nameSuffix.indexOf( param2.param() ) );
        assertTrue( nameSuffix.indexOf( param2.param() ) < nameSuffix.indexOf( param3.param() ) );
        assertTrue( nameSuffix.indexOf( param3.param() ) < nameSuffix.indexOf( paramThreads.param() ) );
        assertTrue( format( "Incorrect name with Seed: %s%n", seed ),
                nameSuffix.endsWith( "(" + MODE_PARAM + ",LATENCY)" ) );
    }
}
