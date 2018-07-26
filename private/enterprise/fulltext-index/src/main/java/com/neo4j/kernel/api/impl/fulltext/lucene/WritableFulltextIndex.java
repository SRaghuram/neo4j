/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import com.neo4j.kernel.api.impl.fulltext.IndexUpdateSink;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

class WritableFulltextIndex extends WritableAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader>
{
    private final IndexUpdateSink indexUpdateSink;

    WritableFulltextIndex( PartitionedIndexStorage storage, WritableIndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            IndexUpdateSink indexUpdateSink )
    {
        super( new LuceneFulltextIndex( storage, partitionFactory, descriptor ) );
        this.indexUpdateSink = indexUpdateSink;
    }

    @Override
    public String toString()
    {
        return luceneIndex.toString();
    }

    @Override
    protected void commitLockedFlush() throws IOException
    {
        if ( indexUpdateSink != null )
        {
            indexUpdateSink.awaitUpdateApplication();
        }
        super.commitLockedFlush();
    }

    @Override
    protected void commitLockedClose() throws IOException
    {
        if ( indexUpdateSink != null )
        {
            indexUpdateSink.awaitUpdateApplication();
        }
        super.commitLockedClose();
    }
}
