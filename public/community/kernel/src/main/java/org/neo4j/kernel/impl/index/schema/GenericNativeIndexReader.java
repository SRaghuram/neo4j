/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

class GenericNativeIndexReader extends NativeIndexReader<CompositeGenericKey,NativeIndexValue>
{
    GenericNativeIndexReader( GBPTree<CompositeGenericKey,NativeIndexValue> tree, Layout<CompositeGenericKey,NativeIndexValue> layout,
            IndexSamplingConfig samplingConfig, IndexDescriptor descriptor )
    {
        super( tree, layout, samplingConfig, descriptor );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        // TODO except spatial tho
        return true;
    }

    @Override
    void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException();
        }

        CapabilityValidator.validateQuery( GenericNativeIndexProvider.CAPABILITY, indexOrder, predicates );
    }

    @Override
    boolean initializeRangeForQuery( CompositeGenericKey treeKeyFrom, CompositeGenericKey treeKeyTo, IndexQuery[] predicates )
    {
        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            treeKeyFrom.initAsLowest( ValueGroup.UNKNOWN );
            treeKeyTo.initAsHighest( ValueGroup.UNKNOWN );
            return false;
        case exact:
            ExactPredicate exactPredicate = (ExactPredicate) predicate;
            treeKeyFrom.from( Long.MIN_VALUE, exactPredicate.value() );
            treeKeyTo.from( Long.MAX_VALUE, exactPredicate.value() );
            return false;
        case range:
            RangePredicate<?> rangePredicate = (RangePredicate<?>) predicate;
            initFromForRange( rangePredicate, treeKeyFrom );
            initToForRange( rangePredicate, treeKeyTo );
            return false;
        case stringPrefix:
            StringPrefixPredicate prefixPredicate = (StringPrefixPredicate) predicate;
            treeKeyFrom.initAsPrefixLow( prefixPredicate.prefix() );
            treeKeyTo.initAsPrefixHigh( prefixPredicate.prefix() );
            return false;
        case stringSuffix:
        case stringContains:
            treeKeyFrom.initAsLowest( ValueGroup.TEXT );
            treeKeyTo.initAsHighest( ValueGroup.TEXT );
            return true;
        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
    }

    private void initFromForRange( RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyFrom )
    {
        Value fromValue = rangePredicate.fromValue();
        if ( fromValue == Values.NO_VALUE )
        {
            treeKeyFrom.initAsLowest( ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyFrom.from( rangePredicate.fromInclusive() ? Long.MIN_VALUE : Long.MAX_VALUE, fromValue );
            treeKeyFrom.setCompareId( true );
        }
    }

    private void initToForRange( RangePredicate<?> rangePredicate, CompositeGenericKey treeKeyTo )
    {
        Value toValue = rangePredicate.toValue();
        if ( toValue == Values.NO_VALUE )
        {
            treeKeyTo.initAsHighest( ValueGroup.UNKNOWN );
        }
        else
        {
            treeKeyTo.from( rangePredicate.toInclusive() ? Long.MAX_VALUE : Long.MIN_VALUE, toValue );
            treeKeyTo.setCompareId( true );
        }
    }
}
