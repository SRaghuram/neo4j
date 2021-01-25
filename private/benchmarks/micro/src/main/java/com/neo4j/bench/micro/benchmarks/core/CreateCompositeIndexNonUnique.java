/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Label;

import static com.neo4j.bench.micro.data.DataGenerator.createSchemaIndex;
import static com.neo4j.bench.micro.data.DataGenerator.dropSchemaIndex;
import static com.neo4j.bench.micro.data.DataGenerator.waitForSchemaIndexes;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteFor;

public class CreateCompositeIndexNonUnique extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final int NODE_COUNT = 10_000_000;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @ParamValues(
            allowed = {"2", "4", "8"},
            base = {"2", "8"} )
    @Param( {} )
    public int keys;

    @Override
    public String description()
    {
        return "Tests performance of composite index creation: Schema.\n" +
               "Benchmark generates a store with nodes and properties.\n" +
               "Each node has the same number of properties as there are elements in the composite key.\n" +
               "Property values assigned with skewed policy, there are three, with frequency 1:10:100.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    private String[] keys()
    {
        return Arrays.stream( propertyDefinitions() ).map( PropertyDefinition::key ).toArray( String[]::new );
    }

    private PropertyDefinition[] propertyDefinitions()
    {
        return IntStream.range( 0, keys )
                        .mapToObj( i -> new PropertyDefinition(
                                type + "_" + i,
                                valuesFor( type ) ) )
                        .toArray( PropertyDefinition[]::new );
    }

    private ValueGeneratorFactory valuesFor( String type )
    {
        double[] discreteBucketRatios = new double[]{1, 10, 100};
        return discreteFor( type, discreteBucketRatios );
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        DataGeneratorConfigBuilder configBuilder = new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinitions() )
                .isReusableStore( false );
        return configBuilder.build();
    }

    @TearDown( Level.Iteration )
    public void dropIndex()
    {
        dropSchemaIndex( db(), LABEL, keys() );
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void createIndex()
    {
        createSchemaIndex( db(), LABEL, keys() );
        waitForSchemaIndexes( db(), LABEL );
    }
}
