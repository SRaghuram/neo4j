/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.IndexType;
import com.neo4j.bench.micro.data.LabelKeyDefinition;
import com.neo4j.bench.micro.data.ManagedStore;
import com.neo4j.bench.micro.data.PropertyDefinition;
import com.neo4j.bench.micro.data.RelationshipDefinition;
import com.neo4j.bench.micro.data.ValueGeneratorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.TearDown;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import static com.neo4j.bench.micro.data.IndexType.SCHEMA;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML_ARR;

@BenchmarkEnabled( true )
public class StartupDb extends AbstractCoreBenchmark
{
    private static final int RELATIONSHIPS_PER_NODE = 2;
    private static final int NODE_COUNT = 10_000_000;
    private static final RelationshipDefinition RELATIONSHIP_DEFINITION =
            new RelationshipDefinition( RelationshipType.withName( "REL" ), RELATIONSHIPS_PER_NODE );
    private static final Label LABEL = Label.label( "Label" );
    private static final String[] PROPERTIES = new String[]{LNG, STR_SML, STR_SML_ARR};

    @ParamValues(
            allowed = {"NONE", "SCHEMA"},
            base = {"NONE", "SCHEMA"} )
    @Param( {} )
    public IndexType index;

    @Override
    protected DataGeneratorConfig getConfig()
    {
        PropertyDefinition[] propertyDefinitions = propertyDefinitions();
        LabelKeyDefinition[] schemaDefinitions = schemaDefinitions();
        return new DataGeneratorConfigBuilder()
                .withNodeCount( NODE_COUNT )
                .withNodeProperties( propertyDefinitions )
                .withLabels( LABEL )
                .withSchemaIndexes( schemaDefinitions )
                .withOutRelationships( RELATIONSHIP_DEFINITION )
                .isReusableStore( true )
                .build();
    }

    private PropertyDefinition[] propertyDefinitions()
    {
        PropertyDefinition[] propertyDefinitions = new PropertyDefinition[PROPERTIES.length];
        for ( int i = 0; i < PROPERTIES.length; i++ )
        {
            propertyDefinitions[i] = ValueGeneratorUtil.randPropertyFor( PROPERTIES[i] );
        }
        return propertyDefinitions;
    }

    private LabelKeyDefinition[] schemaDefinitions()
    {
        LabelKeyDefinition[] labelKeyDefinitions = index.equals( SCHEMA ) ?
                                                   new LabelKeyDefinition[PROPERTIES.length] :
                                                   new LabelKeyDefinition[0];
        for ( int i = 0; i < labelKeyDefinitions.length; i++ )
        {
            labelKeyDefinitions[i] = new LabelKeyDefinition( LABEL, PROPERTIES[i] );
        }
        return labelKeyDefinitions;
    }

    @Override
    public String description()
    {
        return "Test time to start or shutdown a db";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected StartDatabaseInstruction afterDataGeneration()
    {
        // do not start database. this benchmark will manage starting/stopping itself
        return StartDatabaseInstruction.DO_NOT_START_DB;
    }

    @TearDown( Level.Iteration )
    public void tearDownIteration()
    {
        ManagedStore.getManagementService().shutdown();
    }

    @Benchmark
    @BenchmarkMode( Mode.SingleShotTime )
    public void startup()
    {
        managedStore.startDb();
    }
}
