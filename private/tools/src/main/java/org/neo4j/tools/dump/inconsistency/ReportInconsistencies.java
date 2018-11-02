/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump.inconsistency;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

/**
 * Entity ids that reported to be inconsistent in consistency report where they where extracted from.
 */
public class ReportInconsistencies implements Inconsistencies
{
    private final MutableLongSet schemaIndexesIds = new LongHashSet();
    private final MutableLongSet relationshipIds = new LongHashSet();
    private final MutableLongSet nodeIds = new LongHashSet();
    private final MutableLongSet propertyIds = new LongHashSet();
    private final MutableLongSet relationshipGroupIds = new LongHashSet();

    @Override
    public void relationshipGroup( long id )
    {
        relationshipGroupIds.add( id );
    }

    @Override
    public void schemaIndex( long id )
    {
        schemaIndexesIds.add( id );
    }

    @Override
    public void relationship( long id )
    {
        relationshipIds.add( id );
    }

    @Override
    public void property( long id )
    {
        propertyIds.add( id );
    }

    @Override
    public void node( long id )
    {
        nodeIds.add( id );
    }

    @Override
    public boolean containsNodeId( long id )
    {
        return nodeIds.contains( id );
    }

    @Override
    public boolean containsRelationshipId( long id )
    {
        return relationshipIds.contains( id );
    }

    @Override
    public boolean containsPropertyId( long id )
    {
        return propertyIds.contains( id );
    }

    @Override
    public boolean containsRelationshipGroupId( long id )
    {
        return relationshipGroupIds.contains( id );
    }

    @Override
    public boolean containsSchemaIndexId( long id )
    {
        return schemaIndexesIds.contains( id );
    }
}
