/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.core;

import com.neo4j.bench.micro.data.DataGeneratorConfig;
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder;
import com.neo4j.bench.micro.data.PropertyDefinition;

import org.neo4j.graphdb.Label;

public abstract class AbstractCreateIndex extends AbstractCoreBenchmark
{
    protected static final Label LABEL = Label.label( "Label" );

    abstract int nodeCount();

    abstract String getType();

    abstract PropertyDefinition getPropertyDefinition( String type );

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected DataGeneratorConfig getConfig()
    {
        PropertyDefinition propertyDefinition = getPropertyDefinition( getType() );
        DataGeneratorConfigBuilder configBuilder = new DataGeneratorConfigBuilder()
                .withNodeCount( nodeCount() )
                .withLabels( LABEL )
                .withNodeProperties( propertyDefinition )
                .isReusableStore( false );
        return configBuilder.build();
    }
}
