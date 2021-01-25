/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.google.common.base.CaseFormat;

import java.util.Arrays;

import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

public class LabelCamelCaseDecorator implements Decorator
{
    @Override
    public InputEntityVisitor apply( InputEntityVisitor inputEntityVisitor )
    {
        return new InputEntityVisitor.Delegate( inputEntityVisitor )
        {
            @Override
            public boolean labels( String[] labels )
            {
                labels = Arrays.stream( labels )
                        .map( label -> CaseFormat.LOWER_CAMEL.to( CaseFormat.UPPER_CAMEL, label ) )
                        .toArray( String[]::new );
                return super.labels( labels );
            }
        };
    }
}
