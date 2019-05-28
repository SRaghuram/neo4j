/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

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
        if ( value != NO_VALUE && !(value instanceof BooleanValue) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    public static Value cachedNodeProperty(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int nodeOffset,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
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
                propertyOrNull = ctx.getCachedPropertyAt( propertyOffset );
                if ( propertyOrNull == null )
                {
                    propertyOrNull = dbAccess.nodeProperty( nodeId, propertyKey, nodeCursor, propertyCursor );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                }
            }
            return propertyOrNull;
        }
    }

    public static Value cachedRelationshipProperty(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int relOffset,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        long relId = ctx.getLongAt( relOffset );
        if ( relId == StatementConstants.NO_SUCH_RELATIONSHIP || propertyKey == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return NO_VALUE;
        }
        else
        {
            Value propertyOrNull = dbAccess.getTxStateRelationshipPropertyOrNull( relId, propertyKey );
            if ( propertyOrNull == null )
            {
                propertyOrNull = ctx.getCachedPropertyAt( propertyOffset );
                if ( propertyOrNull == null )
                {
                    propertyOrNull = dbAccess.relationshipProperty( relId, propertyKey, relCursor, propertyCursor );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                }
            }
            return propertyOrNull;
        }
    }

    public static Value cachedNodePropertyExists( ExecutionContext ctx, DbAccess dbAccess, int nodeOffset, int propertyKey )
    {
        long nodeId = ctx.getLongAt( nodeOffset );
        if ( nodeId == StatementConstants.NO_SUCH_NODE )
        {
            return NO_VALUE;
        }
        else
        {
            return Values.booleanValue( dbAccess.hasTxStatePropertyForCachedNodeProperty( nodeId, propertyKey ) );
        }
    }

    public static Value cachedRelationshipPropertyExists( ExecutionContext ctx, DbAccess dbAccess, int relOffset, int propertyKey )
    {
        long relId = ctx.getLongAt( relOffset );
        if ( relId == StatementConstants.NO_SUCH_RELATIONSHIP )
        {
            return NO_VALUE;
        }
        else
        {
            return Values.booleanValue( dbAccess.hasTxStatePropertyForCachedRelationshipProperty( relId, propertyKey ) );
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
}
