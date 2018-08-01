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
    private final String analyzerName;
    private final boolean eventuallyConsistent;

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor, Set<String> propertyNames, Analyzer analyzer, String analyzerName, boolean eventuallyConsistent )
    {
        super( descriptor );
        this.propertyNames = propertyNames;
        this.analyzer = analyzer;
        this.analyzerName = analyzerName;
        this.eventuallyConsistent = eventuallyConsistent;
    }

    public Collection<String> propertyNames()
    {
        return propertyNames;
    }

    public Analyzer analyzer()
    {
        return analyzer;
    }

    public String analyzerName()
    {
        return analyzerName;
    }

    public boolean isEventuallyConsistent()
    {
        return eventuallyConsistent;
    }
}
