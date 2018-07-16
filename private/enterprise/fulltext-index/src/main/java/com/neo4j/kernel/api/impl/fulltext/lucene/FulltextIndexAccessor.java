/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;

import java.io.IOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

public class FulltextIndexAccessor extends AbstractLuceneIndexAccessor<FulltextIndexReader,DatabaseIndex<FulltextIndexReader>>
{
    private final FulltextIndexDescriptor descriptor;

    public FulltextIndexAccessor(DatabaseIndex<FulltextIndexReader> luceneIndex, FulltextIndexDescriptor descriptor )
    {
        super( luceneIndex, descriptor );
        this.descriptor = descriptor;
    }

    @Override
    public FulltextIndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        return new FulltextIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return super.newAllEntriesReader( LuceneFulltextDocumentStructure::getNodeId );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor propertyAccessor )
    {
        //The fulltext index does not care about constraints.
    }

    private class FulltextIndexUpdater extends AbstractLuceneIndexUpdater
    {

        private FulltextIndexUpdater( boolean idempotent, boolean refresh )
        {
            super( idempotent, refresh );
        }

        protected void addIdempotent( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void add( long nodeId, Value[] values ) throws IOException
        {
            writer.addDocument( documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void change( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void remove( long nodeId ) throws IOException
        {
            writer.deleteDocuments( newTermForChangeOrRemove( nodeId ) );
        }
    }
}
