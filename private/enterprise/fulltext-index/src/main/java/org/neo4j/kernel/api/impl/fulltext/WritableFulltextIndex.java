/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;

class WritableFulltextIndex extends WritableAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader> implements DatabaseFulltextIndex
{
    private final IndexUpdateSink indexUpdateSink;

    WritableFulltextIndex( IndexUpdateSink indexUpdateSink, LuceneFulltextIndex fulltextIndex )
    {
        super( fulltextIndex );
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
        indexUpdateSink.awaitUpdateApplication();
        super.commitLockedFlush();
    }

    @Override
    protected void commitLockedClose() throws IOException
    {
        indexUpdateSink.awaitUpdateApplication();
        super.commitLockedClose();
    }

    @Override
    public TransactionStateLuceneIndexWriter getTransactionalIndexWriter() throws IOException
    {
        return new TransactionStateLuceneIndexWriter( luceneIndex );
    }
}
