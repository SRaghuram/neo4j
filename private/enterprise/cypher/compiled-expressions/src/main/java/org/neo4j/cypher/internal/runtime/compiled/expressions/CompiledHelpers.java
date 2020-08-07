/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.runtime.DbAccess;
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
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static java.lang.String.format;
import static org.neo4j.cypher.operations.CypherCoercions.asStorableValue;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Contains helper methods used from compiled expressions
 */
@SuppressWarnings( {"unused"} )
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
    public static AnyValue nodeOrNoValue( CypherRow context, DbAccess dbAccess, int offset )
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
    public static AnyValue relationshipOrNoValue( CypherRow context, DbAccess dbAccess, int offset )
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
    public static IndexQuery exactSeek( int property, AnyValue value )
    {
        return IndexQuery.exact( property, asStorableValue( value ) );
    }

    @CalledFromGeneratedCode
    public static IndexQuery lessThanSeek( int property, AnyValue value, boolean inclusive )
    {
        return IndexQuery.range( property, null, false, asStorableValue( value ), inclusive );
    }

    @CalledFromGeneratedCode
    public static IndexQuery multipleLessThanSeek( int property, AnyValue[] values,
            boolean[] inclusive )
    {
        assert values.length == inclusive.length;
        assert values.length > 0;

        int index = seekMinRangeAnConvertToStorable( values, inclusive );
        if ( index >= 0 )
        {
            return IndexQuery.range( property, null, false, (Value) values[index], inclusive[index] );
        }
        else
        {
            return null;
        }
    }

    @CalledFromGeneratedCode
    public static IndexQuery greaterThanSeek( int property, AnyValue value, boolean inclusive )
    {
        return IndexQuery.range( property, asStorableValue( value ), inclusive, null, false );
    }

    @CalledFromGeneratedCode
    public static IndexQuery multipleGreaterThanSeek( int property, AnyValue[] values, boolean[] inclusive )
    {
        assert values.length == inclusive.length;
        assert values.length > 0;

        int index = seekMaxRangeAnConvertToStorable( values, inclusive );
        if ( index >= 0 )
        {
            return IndexQuery.range( property, (Value) values[index], inclusive[index], null, false );
        }
        else
        {
            return null;
        }
    }

    @CalledFromGeneratedCode
    public static IndexQuery rangeBetweenSeek( int property, AnyValue from, boolean fromInclusive, AnyValue to,
            boolean toInclusive )
    {
        return IndexQuery.range( property, asStorableValue( from ), fromInclusive,  asStorableValue( to ), toInclusive );
    }

    @CalledFromGeneratedCode
    public static IndexQuery multipleRangeBetweenSeek( int property, AnyValue[] gtValues, boolean[] gtInclusive,
            AnyValue[] ltValues, boolean[] ltInclusive )
    {
        assert gtValues.length == gtInclusive.length;
        assert ltValues.length == ltInclusive.length;
        assert gtValues.length > 0;
        assert ltValues.length > 0;

        int gtIndex = seekMaxRangeAnConvertToStorable( gtValues, gtInclusive );
        if ( gtIndex < 0 )
        {
            return null;
        }
        int ltIndex = seekMinRangeAnConvertToStorable( ltValues, ltInclusive );
        if ( ltIndex < 0 )
        {
            return null;
        }
        return IndexQuery.range( property, (Value) gtValues[gtIndex], gtInclusive[gtIndex], (Value) ltValues[ltIndex],
                ltInclusive[ltIndex] );
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
            // The geographic calculator pads the range to avoid numerical errors, which means we rely more on post-filtering
            // This also means we can fix the date-line '<' case by simply being inclusive
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
        Set<AnyValue> seen = new HashSet<>();
        ListValueBuilder builder = new ListValueBuilder.UnknownSizeListValueBuilder();
        ListValue seekValuesAsList = CypherFunctions.asList( seekValues );
        for ( AnyValue value : seekValuesAsList )
        {
            if ( value != NO_VALUE && seen.add( value ) )
            {
                builder.add( value );
            }
        }
        ListValue listOfSeekValues = builder.build();
        final int size = listOfSeekValues.size();
        IndexQuery[] predicates = new IndexQuery[size];
        for ( int i = 0; i < size; i++ )
        {
            predicates[i] = IndexQuery.exact( property, asStorableValue( listOfSeekValues.value( i ) ) );
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
    public static long relationshipFromAnyValue( AnyValue value )
    {
        if ( value instanceof VirtualRelationshipValue )
        {
            return ((VirtualRelationshipValue) value).id();
        }
        else
        {
            throw new ParameterWrongTypeException(
                    format( "Expected to find a relationship but found %s instead", value ) );
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

    private static int seekMaxRangeAnConvertToStorable( AnyValue[] values, boolean[] inclusive )
    {
        Value max = asStorableValue( values[0] );
        boolean isInclusive = inclusive[0];
        int found = 0;
        for ( int i = 1; i < values.length; i++ )
        {
            Value value = asStorableValue( values[i] );
            //asStorableValue can be slow for Lists et al so we only want to do it once
            //which is why we mutate in place here
            values[i] = value;

            // comparing different value types always lead to no results
            if ( !max.valueGroup().equals( value.valueGroup() ) )
            {
                return -1;
            }

            int compare = Values.COMPARATOR.compare( max, value );
            if ( compare < 0 )
            {   //value was greater than max
                max = value;
                isInclusive = inclusive[i];
                found = i;
            }
            else if ( compare == 0 && isInclusive && !inclusive[i] )
            { //say that we had >= 4 and now see a > 4
                isInclusive = false;
                found = i;
            }
        }

        return found;
    }

    private static int seekMinRangeAnConvertToStorable( AnyValue[] values, boolean[] inclusive )
    {
        Value min = asStorableValue( values[0] );
        boolean isInclusive = inclusive[0];
        int found = 0;
        for ( int i = 1; i < values.length; i++ )
        {
            Value value = asStorableValue( values[i] );
            //asStorableValue can be slow for Lists et al so we only want to do it once
            //which is why we mutate in place here
            values[i] = value;

            // comparing different value types always lead to no results
            if ( !min.valueGroup().equals( value.valueGroup() ) )
            {
                return -1;
            }

            int compare = Values.COMPARATOR.compare( min, value );
            if ( compare > 0 )
            {
                min = value;
                isInclusive = inclusive[i];
                found = i;
            }
            else if ( compare == 0 && isInclusive && !inclusive[i] )
            { //say that we had <= 4 and now see a < 4
                isInclusive = false;
                found = i;
            }
        }

        return found;
    }
}
