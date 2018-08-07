/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import com.neo4j.kernel.api.impl.fulltext.IndexUpdateSink;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

public class FulltextIndexAccessor extends AbstractLuceneIndexAccessor<FulltextIndexReader,DatabaseIndex<FulltextIndexReader>>
{
    private final IndexUpdateSink indexUpdateSink;
    private final FulltextIndexDescriptor descriptor;

    public FulltextIndexAccessor( IndexUpdateSink indexUpdateSink, DatabaseIndex<FulltextIndexReader> luceneIndex, FulltextIndexDescriptor descriptor )
    {
        super( luceneIndex, descriptor );
        this.indexUpdateSink = indexUpdateSink;
        this.descriptor = descriptor;
    }

    @Override
    public IndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        IndexUpdater indexUpdater = new FulltextIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
        if ( descriptor.isEventuallyConsistent() )
        {
            indexUpdater = new EventuallyConsistentIndexUpdater( luceneIndex, indexUpdater, indexUpdateSink );
        }
        return indexUpdater;
    }

    @Override
    public void close()
    {
        if ( descriptor.isEventuallyConsistent() )
        {
            indexUpdateSink.awaitUpdateApplication();
        }
        super.close();
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

        @Override
        protected void addIdempotent( long nodeId, Value[] values )
        {
            try
            {
                writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void add( long nodeId, Value[] values )
        {
            try
            {
                writer.addDocument( documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void change( long nodeId, Value[] values )
        {
            try
            {
                writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void remove( long nodeId )
        {
            try
            {
                writer.deleteDocuments( newTermForChangeOrRemove( nodeId ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
