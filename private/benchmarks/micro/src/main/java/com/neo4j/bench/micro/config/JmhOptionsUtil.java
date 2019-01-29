package com.neo4j.bench.micro.config;

import com.google.common.collect.Sets;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.micro.data.Stores;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class JmhOptionsUtil
{
    // global JMH params
    private static final String PARAM_NEO4J_CONFIG = "baseNeo4jConfig";
    private static final String PARAM_STORES_DIR = "storesDir";
    public static final Set<String> GLOBAL_PARAMS = Sets.newHashSet(
            PARAM_NEO4J_CONFIG,
            PARAM_STORES_DIR );

    private static final TimeValue DEFAULT_ITERATION_DURATION = TimeValue.seconds( 5 );
    private static final int DEFAULT_ITERATION_COUNT = 5;
    private static final int DEFAULT_FORK_COUNT = 3;

    public static Stores getStores( BenchmarkParams benchmarkParams )
    {
        Path storesDir = Paths.get( benchmarkParams.getParam( PARAM_STORES_DIR ) );
        return new Stores( storesDir );
    }

    public static ChainedOptionsBuilder baseBuilder(
            String neo4jConfigString,
            Stores stores,
            BenchmarkDescription benchmark,
            int threadCount,
            Jvm jvm,
            String... jvmArgs )
    {
        if ( 1 != benchmark.explode().size() )
        {
            throw new Kaboom( "Only one benchmark should be run at a time\n" +
                              "But benchmark: " + benchmark + "\n" +
                              "Explodes to: " + benchmark.explode() );
        }
        // Set options explicitly, even if it is same value as defaults, to guard against defaults changing
        // For defaults see: org.openjdk.jmh.runner.Defaults
        ChainedOptionsBuilder baseBuilder = new OptionsBuilder()
                // Fail entire run on first benchmark error (Default == false)
                .shouldFailOnError( true )
                // Do GC between measurementIterations (Default == false)
                .shouldDoGC( false )
                .warmupIterations( DEFAULT_ITERATION_COUNT )
                .warmupTime( DEFAULT_ITERATION_DURATION )
                .measurementIterations( DEFAULT_ITERATION_COUNT )
                .measurementTime( DEFAULT_ITERATION_DURATION )
                .timeUnit( TimeUnit.MILLISECONDS )
                // Thread that runs first warmup iteration also does setup, which includes data generation
                // Set sufficiently high timeout so data generation never gets interrupted
                .timeout( TimeValue.hours( 1 ) )
                // default already, just making explicit (Default == true)
                .syncIterations( true )
                .jvm( jvm.launchJava() )
                .jvmArgs( jvmArgs )
                // Number of warmup forks to discard (Default == 0)
                .warmupForks( 0 )
                .forks( DEFAULT_FORK_COUNT )
                .threads( threadCount )
                // Do individual warmup for every benchmark (Default == WarmupMode.INDI)
                .warmupMode( WarmupMode.INDI )
                .param( PARAM_NEO4J_CONFIG, neo4jConfigString )
                .param( PARAM_STORES_DIR, stores.storesDir().toFile().getAbsolutePath() );

        if ( benchmark.isEnabled() && (threadCount == 1 || benchmark.isThreadSafe()) )
        {
            for ( BenchmarkParamDescription param : benchmark.parameters().values() )
            {
                String paramName = fullParamName( benchmark, param );
                baseBuilder = baseBuilder.param( paramName, param.valuesArray() );
            }
            for ( BenchmarkMethodDescription method : benchmark.methods() )
            {
                baseBuilder = baseBuilder.include( asRegex( benchmark.className(), method.name() ) );
            }
        }
        else
        {
            for ( BenchmarkMethodDescription method : benchmark.methods() )
            {
                baseBuilder = baseBuilder.exclude( asRegex( benchmark.className(), method.name() ) );
            }
        }

        return baseBuilder;
    }

    public static ChainedOptionsBuilder applyOptions( ChainedOptionsBuilder optionsBuilder, Options options )
    {
        options.getIncludes().forEach( optionsBuilder::include );
        options.getExcludes().forEach( optionsBuilder::exclude );
        options.getProfilers().forEach( pc -> optionsBuilder.addProfiler( pc.getKlass() ) );
        options.getWarmupIncludes().forEach( optionsBuilder::includeWarmup );
        options.getBenchModes().forEach( optionsBuilder::mode );
        // TODO not sure how to extract all parameters without knowing their names up front
        //Optional<Collection<String>> getParameter (String name);
        if ( options.getOutput().hasValue() )
        {
            optionsBuilder.output( options.getOutput().get() );
        }
        if ( options.getResultFormat().hasValue() )
        {
            optionsBuilder.resultFormat( options.getResultFormat().get() );
        }
        if ( options.getResult().hasValue() )
        {
            optionsBuilder.result( options.getResult().get() );
        }
        if ( options.shouldDoGC().hasValue() )
        {
            optionsBuilder.shouldDoGC( options.shouldDoGC().get() );
        }
        if ( options.verbosity().hasValue() )
        {
            optionsBuilder.verbosity( options.verbosity().get() );
        }
        if ( options.shouldFailOnError().hasValue() )
        {
            optionsBuilder.shouldFailOnError( options.shouldFailOnError().get() );
        }
        if ( options.getThreads().hasValue() )
        {
            optionsBuilder.threads( options.getThreads().get() );
        }
        if ( options.getThreadGroups().hasValue() )
        {
            optionsBuilder.threadGroups( options.getThreadGroups().get() );
        }
        if ( options.shouldSyncIterations().hasValue() )
        {
            optionsBuilder.syncIterations( options.shouldSyncIterations().get() );
        }
        if ( options.getWarmupIterations().hasValue() )
        {
            optionsBuilder.warmupIterations( options.getWarmupIterations().get() );
        }
        if ( options.getWarmupTime().hasValue() )
        {
            optionsBuilder.warmupTime( options.getWarmupTime().get() );
        }
        if ( options.getWarmupBatchSize().hasValue() )
        {
            optionsBuilder.warmupBatchSize( options.getWarmupBatchSize().get() );
        }
        if ( options.getWarmupMode().hasValue() )
        {
            optionsBuilder.warmupMode( options.getWarmupMode().get() );
        }
        if ( options.getTimeUnit().hasValue() )
        {
            optionsBuilder.timeUnit( options.getTimeUnit().get() );
        }
        if ( options.getOperationsPerInvocation().hasValue() )
        {
            optionsBuilder.operationsPerInvocation( options.getOperationsPerInvocation().get() );
        }
        if ( options.getForkCount().hasValue() )
        {
            optionsBuilder.forks( options.getForkCount().get() );
        }
        if ( options.getWarmupForkCount().hasValue() )
        {
            optionsBuilder.warmupForks( options.getWarmupForkCount().get() );
        }
        if ( options.getJvm().hasValue() )
        {
            optionsBuilder.jvm( options.getJvm().get() );
        }
        if ( options.getJvmArgs().hasValue() )
        {
            optionsBuilder.jvmArgs( options.getJvmArgs().get().stream().toArray( String[]::new ) );
        }
        if ( options.getJvmArgsAppend().hasValue() )
        {
            optionsBuilder.jvmArgsAppend( options.getJvmArgsAppend().get().stream().toArray( String[]::new ) );
        }
        if ( options.getJvmArgsPrepend().hasValue() )
        {
            optionsBuilder.jvmArgsPrepend( options.getJvmArgsPrepend().get().stream().toArray( String[]::new ) );
        }
        if ( options.getTimeout().hasValue() )
        {
            optionsBuilder.timeout( options.getTimeout().get() );
        }
        if ( options.getMeasurementIterations().hasValue() )
        {
            optionsBuilder.measurementIterations( options.getMeasurementIterations().get() );
        }
        if ( options.getMeasurementTime().hasValue() )
        {
            optionsBuilder.measurementTime( options.getMeasurementTime().get() );
        }
        if ( options.getMeasurementBatchSize().hasValue() )
        {
            optionsBuilder.measurementBatchSize( options.getMeasurementBatchSize().get() );
        }
        return optionsBuilder;
    }

    public static void applyAnnotations( Class<?> benchmark, ChainedOptionsBuilder builder )
    {
        // TODO would be nice if we could do this on method basis
        Warmup warmup = benchmark.getAnnotation( Warmup.class );
        if ( warmup != null )
        {
            if ( warmup.iterations() != Measurement.BLANK_ITERATIONS )
            {
                builder.warmupIterations( warmup.iterations() );
            }
            if ( warmup.time() != Measurement.BLANK_TIME )
            {
                builder.warmupTime( new TimeValue( warmup.time(), warmup.timeUnit() ) );
            }
            if ( warmup.batchSize() != Measurement.BLANK_BATCHSIZE )
            {
                builder.warmupBatchSize( warmup.batchSize() );
            }
        }
        Measurement measurement = benchmark.getAnnotation( Measurement.class );
        if ( measurement != null )
        {
            if ( measurement.iterations() != Measurement.BLANK_ITERATIONS )
            {
                builder.measurementIterations( measurement.iterations() );
            }
            if ( measurement.time() != Measurement.BLANK_TIME )
            {
                builder.measurementTime( new TimeValue( measurement.time(), measurement.timeUnit() ) );
            }
            if ( measurement.batchSize() != Measurement.BLANK_BATCHSIZE )
            {
                builder.measurementBatchSize( measurement.batchSize() );
            }
        }
        OutputTimeUnit timeUnit = benchmark.getAnnotation( OutputTimeUnit.class );
        if ( timeUnit != null )
        {
            builder.timeUnit( timeUnit.value() );
        }
        Threads threads = benchmark.getAnnotation( Threads.class );
        if ( threads != null )
        {
            builder.threads( threads.value() );
        }
    }

    private static String fullParamName( BenchmarkDescription benchmark, BenchmarkParamDescription param )
    {
        return format( "%s_%s", benchmark.simpleName(), param.name() );
    }

    private static String asRegex( String className, String methodName )
    {
        return "\\b" + className + "." + methodName + "\\b";
    }
}
