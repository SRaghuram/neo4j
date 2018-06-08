/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import java.io.IOException;
import java.util.Set;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;

class WritableFulltext extends WritableAbstractDatabaseIndex<LuceneFulltext>
{
    private PartitionedIndexWriter indexWriter;

    WritableFulltext( LuceneFulltext luceneFulltext )
    {
        super( luceneFulltext );
    }

    @Override
    public void open() throws IOException
    {
        super.open();
        indexWriter = luceneIndex.getIndexWriter( this );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        indexWriter = null;
    }

    @Override
    public void drop()
    {
        super.drop();
        indexWriter = null;
    }

    PartitionedIndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    Set<String> getProperties()
    {
        return luceneIndex.getProperties();
    }

    void setPopulated()
    {
        luceneIndex.setPopulated();
    }

    void setFailed()
    {
        luceneIndex.setFailed();
    }

    ReadOnlyFulltext getIndexReader() throws IOException
    {
        return luceneIndex.getIndexReader();
    }

    String getAnalyzerName()
    {
        return luceneIndex.getAnalyzerName();
    }

    void saveConfiguration( long lastCommittedTransactionId ) throws IOException
    {
        luceneIndex.saveConfiguration( lastCommittedTransactionId );
    }

    InternalIndexState getState()
    {
        return luceneIndex.getState();
    }
}
