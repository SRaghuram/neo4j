/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import org.neo4j.io.IOUtils;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.api.impl.fulltext.ScoreEntityIterator.mergeIterators;

class TransactionStateFulltextIndexReader extends FulltextIndexReader
{
    private final FulltextIndexReader baseReader;
    private final FulltextIndexReader nearRealTimeReader;
    private final MutableLongSet modifiedEntityIdsInThisTransaction;

    TransactionStateFulltextIndexReader( FulltextIndexReader baseReader, FulltextIndexReader nearRealTimeReader,
            MutableLongSet modifiedEntityIdsInThisTransaction )
    {
        this.baseReader = baseReader;
        this.nearRealTimeReader = nearRealTimeReader;
        this.modifiedEntityIdsInThisTransaction = modifiedEntityIdsInThisTransaction;
    }

    @Override
    public ScoreEntityIterator query( String query ) throws ParseException
    {
        ScoreEntityIterator iterator = baseReader.query( query );
        iterator = iterator.filter( entry -> !modifiedEntityIdsInThisTransaction.contains( entry.entityId() ) );
        iterator = mergeIterators( asList( iterator, nearRealTimeReader.query( query ) ) );
        return iterator;
    }

    @Override
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        // This is only used in the Consistency Checker. We don't need to worry about this here.
        return 0;
    }

    @Override
    public void close()
    {
        // The 'baseReader' is managed by the kernel, so we don't need to close it here.
        IOUtils.closeAllUnchecked( nearRealTimeReader );
    }
}
