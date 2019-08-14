/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import java.util.Optional;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.runtime.KernelAPISupport$;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;
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
    private static ValueGroup[] RANGE_SEEKABLE_VALUE_GROUPS = KernelAPISupport$.MODULE$.RANGE_SEEKABLE_VALUE_GROUPS();

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    private static Value cachedNodeProperty(
            long nodeId,
            ExecutionContext ctx,
            DbAccess dbAccess,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
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
                    propertyOrNull = dbAccess.nodeProperty( nodeId, propertyKey, nodeCursor, propertyCursor, true );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                }
            }
            return propertyOrNull;
        }
    }

    public static Value cachedNodePropertyWithLongSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int nodeOffset,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
        long nodeId = ctx.getLongAt( nodeOffset );
        return cachedNodeProperty( nodeId, ctx, dbAccess, propertyKey, propertyOffset, nodeCursor, propertyCursor );
    }

    public static Value cachedNodePropertyWithRefSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int nodeOffset,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
        long nodeId = ((VirtualNodeValue) ctx.getRefAt( nodeOffset )).id();
        return cachedNodeProperty( nodeId, ctx, dbAccess, propertyKey, propertyOffset, nodeCursor, propertyCursor );
    }

    private static Value cachedRelationshipProperty(
            long relId,
            ExecutionContext ctx,
            DbAccess dbAccess,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
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
                    propertyOrNull = dbAccess.relationshipProperty( relId, propertyKey, relCursor, propertyCursor, true );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                }
            }
            return propertyOrNull;
        }
    }

    public static Value cachedRelationshipPropertyWithLongSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int relOffset,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        long relId = ctx.getLongAt( relOffset );
        return cachedRelationshipProperty( relId, ctx, dbAccess, propertyKey, propertyOffset, relCursor, propertyCursor );
    }

    public static Value cachedRelationshipPropertyWithRefSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int relOffset,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        long relId = ((VirtualRelationshipValue) ctx.getRefAt( relOffset )).id();
        return cachedRelationshipProperty( relId, ctx, dbAccess, propertyKey, propertyOffset, relCursor, propertyCursor );
    }

    private static Value cachedNodePropertyExists(
            long nodeId,
            ExecutionContext ctx,
            DbAccess dbAccess,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
        if ( nodeId == StatementConstants.NO_SUCH_NODE )
        {
            return NO_VALUE;
        }
        else if ( propertyKey == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.FALSE;
        }
        else
        {
            Optional<Boolean> hasTxChanges = dbAccess.hasTxStatePropertyForCachedNodeProperty( nodeId, propertyKey );
            if ( hasTxChanges.isEmpty() )
            {
                Value propertyOrNull = ctx.getCachedPropertyAt( propertyOffset );
                if ( propertyOrNull == null )
                {
                    // the cached node property has been invalidated
                    propertyOrNull = dbAccess.nodeProperty( nodeId, propertyKey, nodeCursor, propertyCursor, false );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                    return Values.booleanValue( propertyOrNull != NO_VALUE );
                }
                else if ( propertyOrNull == NO_VALUE )
                {
                    return Values.FALSE;
                }
                else
                {
                    return Values.TRUE;
                }
            }
            else
            {
                return Values.booleanValue( hasTxChanges.get() );
            }
        }
    }

    public static Value cachedNodePropertyExistsWithLongSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int nodeOffset,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
        long nodeId = ctx.getLongAt( nodeOffset );
        return cachedNodePropertyExists( nodeId, ctx, dbAccess, propertyKey, propertyOffset, nodeCursor, propertyCursor );
    }

    public static Value cachedNodePropertyExistsWithRefSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int nodeOffset,
            int propertyKey,
            int propertyOffset,
            NodeCursor nodeCursor,
            PropertyCursor propertyCursor )
    {
        long nodeId = ((VirtualNodeValue) ctx.getRefAt( nodeOffset )).id();
        return cachedNodePropertyExists( nodeId, ctx, dbAccess, propertyKey, propertyOffset, nodeCursor, propertyCursor );
    }

    private static Value cachedRelationshipPropertyExists(
            long relId,
            ExecutionContext ctx,
            DbAccess dbAccess,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        if ( relId == StatementConstants.NO_SUCH_RELATIONSHIP )
        {
            return NO_VALUE;
        }
        else if ( propertyKey == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.FALSE;
        }
        else
        {
            Optional<Boolean> hasTxChanges = dbAccess.hasTxStatePropertyForCachedRelationshipProperty( relId, propertyKey );
            if ( hasTxChanges.isEmpty() )
            {
                Value propertyOrNull = ctx.getCachedPropertyAt( propertyOffset );
                if ( propertyOrNull == null )
                {
                    // the cached rel property has been invalidated
                    propertyOrNull = dbAccess.relationshipProperty( relId, propertyKey, relCursor, propertyCursor, false );
                    // Re-cache the value
                    ctx.setCachedPropertyAt( propertyOffset, propertyOrNull );
                    return Values.booleanValue( propertyOrNull != NO_VALUE );
                }
                else if ( propertyOrNull == NO_VALUE )
                {
                    return Values.FALSE;
                }
                else
                {
                    return Values.TRUE;
                }
            }
            else
            {
                return Values.booleanValue( hasTxChanges.get() );
            }
        }
    }

    public static Value cachedRelationshipPropertyExistsWithLongSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int relOffset,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        long relId = ctx.getLongAt( relOffset );
        return cachedRelationshipPropertyExists(relId, ctx, dbAccess, propertyKey, propertyOffset, relCursor, propertyCursor);
    }

    public static Value cachedRelationshipPropertyExistsWithRefSlot(
            ExecutionContext ctx,
            DbAccess dbAccess,
            int relOffset,
            int propertyKey,
            int propertyOffset,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor )
    {
        long relId = ((VirtualRelationshipValue) ctx.getRefAt( relOffset )).id();
        return cachedRelationshipPropertyExists(relId, ctx, dbAccess, propertyKey, propertyOffset, relCursor, propertyCursor);
    }

   public static boolean possibleRangePredicate( IndexQuery query )
   {
       ValueGroup valueGroup = query.valueGroup();

       for ( ValueGroup rangeSeekableValueGroup : RANGE_SEEKABLE_VALUE_GROUPS )
       {
           if ( valueGroup == rangeSeekableValueGroup )
           {
               return true;
           }
       }
       return false;
   }
}
