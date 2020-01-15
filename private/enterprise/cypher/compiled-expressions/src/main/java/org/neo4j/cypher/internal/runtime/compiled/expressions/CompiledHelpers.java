/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import java.util.List;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.runtime.KernelAPISupport$;
import org.neo4j.cypher.operations.CypherFunctions;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.ParameterWrongTypeException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualNodeValue;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Contains helper methods used from compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CompiledHelpers
{
    private static final IndexQuery[] EMPTY_PREDICATE = new IndexQuery[0];

    private CompiledHelpers()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    private static final ValueGroup[] RANGE_SEEKABLE_VALUE_GROUPS = KernelAPISupport$.MODULE$.RANGE_SEEKABLE_VALUE_GROUPS();

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue) )
        {
            throw new CypherTypeException( format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    @CalledFromGeneratedCode
    public static AnyValue nodeOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long nodeId = context.getLongAt( offset );
        return nodeId == -1 ? NO_VALUE : dbAccess.nodeById( nodeId );
    }

    @CalledFromGeneratedCode
    public static AnyValue nodeOrNoValue( DbAccess dbAccess, long nodeId )
    {
        return nodeId == -1 ? NO_VALUE : dbAccess.nodeById( nodeId );
    }

    @CalledFromGeneratedCode
    public static AnyValue relationshipOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long relationshipId = context.getLongAt( offset );
        return relationshipId == -1 ? NO_VALUE : dbAccess.relationshipById( relationshipId );
    }

    @CalledFromGeneratedCode
    public static boolean possibleRangePredicate( IndexQuery query )
    {
        if ( query == null )
        {
            return false;
         }

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

    @CalledFromGeneratedCode
    public static IndexQuery stringPrefix( int property, AnyValue value )
    {
        if ( value == NO_VALUE )
        {
            return null;
        }
        else if ( value instanceof TextValue )
        {
            return IndexQuery.stringPrefix( property, (TextValue) value );
        }
        else
        {
            throw new CypherTypeException( "Expected a string value, but got " + value );
        }
    }

    @CalledFromGeneratedCode
    public static IndexQuery[] pointRange( int property, AnyValue point, AnyValue distance, boolean rangeIsInclusive )
    {
        if ( point instanceof PointValue && distance instanceof NumberValue )
        {
            PointValue pointValue = (PointValue) point;
            List<Pair<PointValue,PointValue>> bboxes = pointValue
                    .getCoordinateReferenceSystem()
                    .getCalculator()
                    .boundingBox( pointValue, ((NumberValue) distance).doubleValue() );
            int size = bboxes.size();
            boolean inclusive = size > 1 || rangeIsInclusive;
            IndexQuery[] queries = new IndexQuery[size];
            for ( int i = 0; i < size; i++ )
            {
                Pair<PointValue,PointValue> bbox = bboxes.get( i );
                queries[i] = IndexQuery.range( property, bbox.first(), inclusive, bbox.other(), inclusive );
            }

            return queries;
        }
        else
        {
            return EMPTY_PREDICATE;
        }
    }

    @CalledFromGeneratedCode
    public static IndexQuery[] manyExactQueries( int property, AnyValue seekValues )
    {
        ListValue listOfSeekValues = CypherFunctions.asList( seekValues ).distinct();
        final int size = listOfSeekValues.size();
        IndexQuery[] predicates = new IndexQuery[size];
        for ( int i = 0; i < size; i++ )
        {
            predicates[i] = IndexQuery.exact( property, listOfSeekValues.value( i ) );
        }
        return predicates;
    }

    @CalledFromGeneratedCode
    public static long nodeFromAnyValue( AnyValue value )
    {
        if ( value instanceof VirtualNodeValue )
        {
            return ((VirtualNodeValue) value).id();
        }
        else
        {
            throw new ParameterWrongTypeException(
                    format( "Expected to find a node but found %s instead", value ) );
        }
    }

    @CalledFromGeneratedCode
    public static long nodeIdOrNullFromAnyValue( AnyValue value )
    {
        if ( value instanceof VirtualNodeValue )
        {
            return ((VirtualNodeValue) value).id();
        }
        else
        {
            return -1L;
        }
    }

}
