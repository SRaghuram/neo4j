/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Contains helper methods used from compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CompiledHelpers
{
    private CompiledHelpers()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue ) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    public static Value cachedProperty( ExecutionContext ctx, DbAccess dbAccess, int nodeOffset, int propertyKey,
            int propertyOffset )
    {
        long nodeId = ctx.getLongAt( nodeOffset );
        if ( nodeId == StatementConstants.NO_SUCH_NODE || propertyKey == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return NO_VALUE;
        }
        else
        {
            Value propertyOrNull = dbAccess.getTxStateNodePropertyOrNull( nodeId, propertyKey );
            if ( propertyOrNull == null )
            {
                return ctx.getCachedPropertyAt( propertyOffset );
            }
            return propertyOrNull;
        }
    }

    public static AnyValue nodeOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long nodeId = context.getLongAt( offset );
        return nodeId == -1 ? NO_VALUE : dbAccess.nodeById( nodeId );
    }

    public static AnyValue relationshipOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long relationshipId = context.getLongAt( offset );
        return relationshipId == -1 ? NO_VALUE : dbAccess.relationshipById( relationshipId );
    }

    public static NodeValue startNode( DbAccess access, VirtualRelationshipValue relationship )
    {
        try ( RelationshipScanCursor cursor = access.singleRelationship( relationship.id() ) )
        {
            return access.nodeById( cursor.sourceNodeReference() );
        }
    }

    public static NodeValue endNode( DbAccess access, VirtualRelationshipValue relationship )
    {
        try ( RelationshipScanCursor cursor = access.singleRelationship( relationship.id() ) )
        {
            return access.nodeById( cursor.targetNodeReference() );
        }
    }

    public static NodeValue otherNode( DbAccess access, VirtualRelationshipValue relationship, VirtualNodeValue node )
    {
        try ( RelationshipScanCursor cursor = access.singleRelationship( relationship.id() ) )
        {
            if ( node.id() == cursor.sourceNodeReference() )
            {
                return access.nodeById( cursor.targetNodeReference() );
            }
            else if ( node.id() == cursor.targetNodeReference() )
            {
                return access.nodeById( cursor.sourceNodeReference() );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid argument, node is not member of relationship" );
            }
        }
    }
}
