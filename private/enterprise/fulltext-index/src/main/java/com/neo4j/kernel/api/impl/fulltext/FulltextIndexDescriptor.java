/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

public class FulltextIndexDescriptor extends StoreIndexDescriptor
{
    private final Analyzer analyzer;
    private final String analyzerName;
    private final boolean eventuallyConsistent;

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor, Analyzer analyzer, String analyzerName, boolean eventuallyConsistent )
    {
        super( descriptor );
        this.analyzer = analyzer;
        this.analyzerName = analyzerName;
        this.eventuallyConsistent = eventuallyConsistent;
    }

    public Collection<String> propertyNames()
    {
        int[] propertyIds = schema().getPropertyIds();
        Collection<String> propertyNames = new ArrayList<>( propertyIds.length );
        for ( int propertyId : propertyIds )
        {
            propertyNames.add( "p" + propertyId );
        }
        return propertyNames;
    }

    public Analyzer analyzer()
    {
        return analyzer;
    }

    String analyzerName()
    {
        return analyzerName;
    }

    public boolean isEventuallyConsistent()
    {
        return eventuallyConsistent;
    }
}
