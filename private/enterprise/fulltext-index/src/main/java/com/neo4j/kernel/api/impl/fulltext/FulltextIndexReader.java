/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

public abstract class FulltextIndexReader implements IndexReader
{
    /**
     * Queires the fulltext index with the given lucene-syntax query
     *
     * @param query the lucene query
     * @return A {@link ScoreEntityIterator} over the results
     */
    public abstract ScoreEntityIterator query( String query ) throws ParseException;

    @Override
    public IndexSampler createSampler()
    {
        return IndexSampler.EMPTY;
    }

    @Override
    public PrimitiveLongResourceIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        throw new IndexNotApplicableKernelException( "Fulltext indexes does not support IndexQuery queries" );
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }
}
