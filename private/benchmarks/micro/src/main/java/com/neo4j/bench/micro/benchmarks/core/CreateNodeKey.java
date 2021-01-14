/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.data.DataGeneratorConfig;
import com.neo4j.bench.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.data.PropertyDefinition;
import com.neo4j.bench.data.ValueGeneratorFactory;
import com.neo4j.bench.data.ValueGeneratorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Label;

import static com.neo4j.bench.data.DataGenerator.createNodeKey;
import static com.neo4j.bench.data.DataGenerator.dropNodeKey;
import static com.neo4j.bench.data.DataGenerator.waitForSchemaIndexes;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE;
import static com.neo4j.bench.data.ValueGeneratorUtil.DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.DURATION;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_DATE_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.LOCAL_TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.POINT;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_BIG_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.data.ValueGeneratorUtil.STR_SML_ARR;
import static com.neo4j.bench.data.ValueGeneratorUtil.TIME;
import static com.neo4j.bench.data.ValueGeneratorUtil.ascGeneratorFor;
import static com.neo4j.bench.data.ValueGeneratorUtil.defaultRangeFor;
import static com.neo4j.bench.data.ValueGeneratorUtil.discreteFor;

@BenchmarkEnabled( true )
public class CreateNodeKey extends AbstractCoreBenchmark
{
    private static final Label LABEL = Label.label( "Label" );
    private static final int NODE_COUNT = 1_000_000;

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
               "Values of one of the property have frequency 5:1:...:1, all other have random values.";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    private String[] keys()
    {
        return IntStream.range( 0, keys )
                        .mapToObj( i -> type + "_" + i )
                        .toArray( String[]::new );
    }

    private PropertyDefinition[] propertyDefinitions()
    {
        List<PropertyDefinition> propertyDefinitions = IntStream.range( 0, keys - 1 )
                                                                .mapToObj( i -> new PropertyDefinition( type + "_" + i,
                                                                                                        valuesFor( type ) ) )
                                                                .collect( Collectors.toList() );
        ValueGeneratorFactory valueGeneratorFactory = discretePropertyValueGenerator();
        propertyDefinitions.add( new PropertyDefinition( type + "_" + (keys - 1),
                                                         valueGeneratorFactory ) );
        return propertyDefinitions.toArray( new PropertyDefinition[0] );
    }

    private ValueGeneratorFactory discretePropertyValueGenerator()
    {
        int singleUnitSize = 9_000;
        int totalNumberOfBuckets = NODE_COUNT / singleUnitSize;
        List<Integer> ratios = new ArrayList<>();
        totalNumberOfBuckets = addCustomRatio( totalNumberOfBuckets, ratios, 5 );
        int typicalBucketSize = 1;
        while ( totalNumberOfBuckets > 0 )
        {
            ratios.add( typicalBucketSize );
            totalNumberOfBuckets -= typicalBucketSize;
        }
        double[] ratiosArray = ratios.stream().mapToDouble( Integer::doubleValue ).toArray();
        return discreteFor( type, ratiosArray );
    }

    private int addCustomRatio( int totalNumberOfBuckets, List<Integer> ratios, int customRatio )
    {
        ratios.add( customRatio );
        return totalNumberOfBuckets - customRatio;
    }

    private ValueGeneratorFactory valuesFor( String type )
    {
        ValueGeneratorUtil.Range range = defaultRangeFor( type );
        return ascGeneratorFor( type, range.min() );
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
        dropNodeKey( db(), LABEL, keys() );
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void createIndex()
    {
        createNodeKey( db(), LABEL, keys() );
        waitForSchemaIndexes( db(), LABEL );
    }
}
