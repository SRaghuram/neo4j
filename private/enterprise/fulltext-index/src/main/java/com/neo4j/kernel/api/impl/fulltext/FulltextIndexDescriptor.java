/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.core.TokenHolder;

public class FulltextIndexDescriptor extends StoreIndexDescriptor
{
    private final Set<String> propertyNames;
    private final String analyzer;

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor, TokenHolder propertyKeyTokenHolder, String analyzer )
    {
        super( descriptor );
        this.analyzer = analyzer;
        Set<String> names = new HashSet<>();
        for ( int propertyId : schema.getPropertyIds() )
        {
            names.add( propertyKeyTokenHolder.getTokenByIdOrNull( propertyId ).name() );
        }
        propertyNames = Collections.unmodifiableSet( names );
    }

    public Collection<String> propertyNames()
    {
        return propertyNames;
    }

    public String analyzer()
    {
        return analyzer;
    }
}
