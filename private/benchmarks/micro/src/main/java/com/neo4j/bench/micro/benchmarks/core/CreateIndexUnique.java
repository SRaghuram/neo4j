/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import com.neo4j.bench.micro.data.IndexType;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorUtil.Range;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;

import static com.neo4j.bench.micro.data.DataGenerator.createMandatoryNodeConstraint;
import static com.neo4j.bench.micro.data.DataGenerator.createSchemaIndex;
import static com.neo4j.bench.micro.data.DataGenerator.dropMandatoryNodeConstraint;
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
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.defaultRangeFor;

@BenchmarkEnabled( true )
public class CreateIndexUnique extends AbstractCreateIndex
{
    @ParamValues(
            allowed = {"SCHEMA", "MANDATORY"},
            base = {"SCHEMA"} )
    @Param( {} )
    public IndexType CreateIndexUnique_index;

    @ParamValues(
            allowed = {
                    INT, LNG, FLT, DBL, STR_SML, STR_BIG,
                    DATE_TIME, LOCAL_DATE_TIME, TIME, LOCAL_TIME, DATE, DURATION, POINT,
                    INT_ARR, LNG_ARR, FLT_ARR, DBL_ARR, STR_SML_ARR, STR_BIG_ARR},
            base = {LNG, STR_SML, DATE_TIME, POINT} )
    @Param( {} )
    public String CreateIndexUnique_type;

    @Override
    int nodeCount()
    {
        return 10_000_000;
    }

    @Override
    String getType()
    {
        return CreateIndexUnique_type;
    }

    @Override
    PropertyDefinition getPropertyDefinition( String type )
    {
        Range range = defaultRangeFor( type );
        return ascPropertyFor( type, range.min() );
    }

    @Override
    public String description()
    {
        return "Tests performance of schema index creation.\n" +
                "Benchmark generates a store with nodes and properties.\n" +
                "Each node has exactly one property and property values are unique.";
    }

    @TearDown( Level.Iteration )
    public void dropIndex()
    {
        switch ( CreateIndexUnique_index )
        {
        case SCHEMA:
            dropSchemaIndex( db(), LABEL, CreateIndexUnique_type );
            break;
        case MANDATORY:
            dropMandatoryNodeConstraint( db(), LABEL, CreateIndexUnique_type );
            break;
        default:
            throw new RuntimeException( "Unrecognized index type: " + CreateIndexUnique_index );
        }
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void createIndex()
    {
        switch ( CreateIndexUnique_index )
        {
        case SCHEMA:
            createSchemaIndex( db(), LABEL, CreateIndexUnique_type );
            break;
        case MANDATORY:
            createMandatoryNodeConstraint( db(), LABEL, CreateIndexUnique_type );
            break;
        default:
            throw new RuntimeException( "Unrecognized index type: " + CreateIndexUnique_index );
        }
        waitForSchemaIndexes( db(), LABEL );
    }
}
