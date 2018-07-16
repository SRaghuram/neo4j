/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.lock.ResourceType;

class FulltextSchemaDescriptor implements SchemaDescriptor
{
    private final SchemaDescriptor schema;
    private final String analyzerClassName;

    FulltextSchemaDescriptor( SchemaDescriptor schema, String analyzerClassName )
    {
        this.schema = schema;
        this.analyzerClassName = analyzerClassName;
    }

    @Override
    public boolean isAffected( long[] entityTokenIds )
    {
        return schema.isAffected( entityTokenIds );
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> computer )
    {
        return schema.computeWith( computer );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        schema.processWith( processor );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return schema.userDescription( tokenNameLookup );
    }

    @Override
    public int[] getPropertyIds()
    {
        return schema.getPropertyIds();
    }

    @Override
    public int getPropertyId()
    {
        return schema.getPropertyId();
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return schema.getEntityTokenIds();
    }

    @Override
    public int keyId()
    {
        return schema.keyId();
    }

    @Override
    public ResourceType keyType()
    {
        return schema.keyType();
    }

    @Override
    public EntityType entityType()
    {
        return schema.entityType();
    }

    @Override
    public SchemaDescriptor.PropertySchemaType propertySchemaType()
    {
        return schema.propertySchemaType();
    }

    @Override
    public SchemaDescriptor schema()
    {
        return this;
    }

    @Override
    public int hashCode()
    {
        return schema.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof FulltextSchemaDescriptor )
        {
            return schema.equals( ((FulltextSchemaDescriptor) obj).schema );
        }
        return schema.equals( obj );
    }

    String getAnalyzerClassName()
    {
        return analyzerClassName;
    }
}
