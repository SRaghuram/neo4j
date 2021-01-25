/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.CRS;
import com.neo4j.bench.micro.data.IndexType;
import com.neo4j.bench.micro.data.PropertyDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;

import static com.neo4j.bench.micro.data.DataGenerator.createSchemaIndex;
import static com.neo4j.bench.micro.data.DataGenerator.dropSchemaIndex;
import static com.neo4j.bench.micro.data.DataGenerator.waitForSchemaIndexes;
import static com.neo4j.bench.micro.data.DiscreteGenerator.discrete;
import static com.neo4j.bench.micro.data.PointGenerator.diagonal;
import static com.neo4j.bench.micro.data.TemporalGenerator.date;
import static com.neo4j.bench.micro.data.TemporalGenerator.dateTime;
import static com.neo4j.bench.micro.data.TemporalGenerator.duration;
import static com.neo4j.bench.micro.data.TemporalGenerator.localDatetime;
import static com.neo4j.bench.micro.data.TemporalGenerator.localTime;
import static com.neo4j.bench.micro.data.TemporalGenerator.time;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor;

@BenchmarkEnabled( true )
public class CreateIndexNonUnique extends AbstractCreateIndex
{
    @ParamValues(
            allowed = {"SCHEMA"},
            base = {"SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML} )
    @Param( {} )
    public String type;

    @Override
    int nodeCount()
    {
        return 10_000_000;
    }

    @Override
    String getType()
    {
        return type;
    }

    @Override
    PropertyDefinition getPropertyDefinition( String type )
    {
        // Store will have three property values, with frequency of 1:10:100
        double[] discreteBucketRatios = new double[]{1, 10, 100};
        switch ( type )
        {
        case DATE_TIME:
            return new PropertyDefinition( type, dateTime( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case LOCAL_DATE_TIME:
            return new PropertyDefinition( type, localDatetime( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case TIME:
            return new PropertyDefinition( type, time( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case LOCAL_TIME:
            return new PropertyDefinition( type, localTime( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case DATE:
            return new PropertyDefinition( type, date( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case DURATION:
            return new PropertyDefinition( type, duration( discrete( discreteBucketsFor( LNG, discreteBucketRatios ) ) ) );
        case POINT:
            return new PropertyDefinition( type, diagonal( discrete( discreteBucketsFor( DBL, discreteBucketRatios ) ), new CRS.Cartesian() ) );
        default:
            return new PropertyDefinition( type, discrete( discreteBucketsFor( type, discreteBucketRatios ) ) );
        }
    }

    @Override
    public String description()
    {
        return "Tests performance of schema index creation.\n" +
               "Benchmark generates a store with nodes and properties.\n" +
               "Each node has exactly one property.\n" +
               "Property values assigned with skewed policy, there are three, with frequency 1:10:100.";
    }

    @TearDown( Level.Iteration )
    public void dropIndex()
    {
        dropSchemaIndex( db(), LABEL, type );
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void createIndex()
    {
        createSchemaIndex( db(), LABEL, type );
        waitForSchemaIndexes( db(), LABEL );
    }
}
