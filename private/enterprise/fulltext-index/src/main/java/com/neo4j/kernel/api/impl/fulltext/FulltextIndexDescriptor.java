/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.util.Collection;
import java.util.Set;

import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;

public class FulltextIndexDescriptor extends StoreIndexDescriptor
{
    private final Set<String> propertyNames;
    private final Analyzer analyzer;

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor, Set<String> propertyNames, Analyzer analyzer )
    {
        super( descriptor );
        this.propertyNames = propertyNames;
        this.analyzer = analyzer;
    }

    public Collection<String> propertyNames()
    {
        return propertyNames;
    }

    public Analyzer analyzer()
    {
        return analyzer;
    }
}
