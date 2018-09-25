/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

import static com.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static com.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

public class FulltextIndexAccessor extends AbstractLuceneIndexAccessor<FulltextIndexReader,DatabaseFulltextIndex>
{
    private final IndexUpdateSink indexUpdateSink;
    private final FulltextIndexDescriptor descriptor;
    private final Runnable onClose;

    public FulltextIndexAccessor( IndexUpdateSink indexUpdateSink, DatabaseFulltextIndex luceneIndex, FulltextIndexDescriptor descriptor,
            Runnable onClose )
    {
        super( luceneIndex, descriptor );
        this.indexUpdateSink = indexUpdateSink;
        this.descriptor = descriptor;
        this.onClose = onClose;
    }

    public FulltextIndexDescriptor getDescriptor()
    {
        return descriptor;
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
        try
        {
            if ( descriptor.isEventuallyConsistent() )
            {
                indexUpdateSink.awaitUpdateApplication();
            }
            super.close();
        }
        finally
        {
            onClose.run();
        }
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

    public TransactionStateLuceneIndexWriter getTransactionStateIndexWriter()
    {
        try
        {
            return luceneIndex.getTransactionalIndexWriter();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public class FulltextIndexUpdater extends AbstractLuceneIndexUpdater
    {
        private FulltextIndexUpdater( boolean idempotent, boolean refresh )
        {
            super( idempotent, refresh );
        }

        @Override
        protected void addIdempotent( long entityId, Value[] values )
        {
            try
            {
                writer.updateDocument( newTermForChangeOrRemove( entityId ), documentRepresentingProperties( entityId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        public void add( long entityId, Value[] values )
        {
            try
            {
                writer.addDocument( documentRepresentingProperties( entityId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void change( long entityId, Value[] values )
        {
            try
            {
                writer.updateDocument( newTermForChangeOrRemove( entityId ), documentRepresentingProperties( entityId, descriptor.propertyNames(), values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void remove( long entityId )
        {
            try
            {
                writer.deleteDocuments( newTermForChangeOrRemove( entityId ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
