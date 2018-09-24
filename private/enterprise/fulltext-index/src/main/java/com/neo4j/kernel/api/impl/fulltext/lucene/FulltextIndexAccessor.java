/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import com.neo4j.kernel.api.impl.fulltext.IndexUpdateSink;
import org.apache.lucene.queryparser.classic.ParseException;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;
import static com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator.concat;
import static java.util.Arrays.asList;

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

    @Override
    public IndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        IndexUpdater indexUpdater = new FulltextIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh(), writer );
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

    public FulltextIndexTransactionStateUpdater getTransactionStateIndexUpdater()
    {
        try
        {
            TransactionStateLuceneIndexWriter transactionalIndexWriter = luceneIndex.getTransactionalIndexWriter();
//            return transactionalIndexWriter;
            return new FulltextIndexTransactionStateUpdater( transactionalIndexWriter );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public class FulltextIndexUpdater extends AbstractLuceneIndexUpdater
    {
        private final LuceneIndexWriter writer;

        private FulltextIndexUpdater( boolean idempotent, boolean refresh, LuceneIndexWriter writer )
        {
            super( idempotent, refresh );
            this.writer = writer;
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

    public class FulltextIndexTransactionStateUpdater extends FulltextIndexUpdater
    {
        private final MutableLongSet modifiedEntityIdsInThisTransaction;
        private final TransactionStateLuceneIndexWriter writer;

        private FulltextIndexTransactionStateUpdater( TransactionStateLuceneIndexWriter writer )
        {
            super( false, true, writer );
            this.writer = writer;
            modifiedEntityIdsInThisTransaction = new LongHashSet();
        }

        public void resetUpdaterState() throws IOException
        {
            modifiedEntityIdsInThisTransaction.clear(); // Clear this so we don't filter out entities who have had their changes reversed since last time.
            writer.resetWriterState();
        }

        @Override
        public void add( long entityId, Value[] values )
        {
            if ( modifiedEntityIdsInThisTransaction.add( entityId ) )
            {
                // We filter out duplicates that may happen, since a node can have both property and label changes in a transaction.
                super.add( entityId, values );
            }
        }

        @Override
        public void close()
        {
            IOUtils.closeAllUnchecked( writer );
        }

        public FulltextIndexReader getTransactionStateIndexReader( FulltextIndexReader baseReader ) throws IOException
        {
            FulltextIndexReader nearRealTimeReader = writer.getNearRealTimeReader();
            return new FulltextIndexReader()
            {
                @Override
                public ScoreEntityIterator query( String query ) throws ParseException
                {
                    ScoreEntityIterator iterator = baseReader.query( query );
                    iterator = iterator.filter( entry -> !modifiedEntityIdsInThisTransaction.contains( entry.entityId() ) );
                    iterator = concat( asList( iterator, nearRealTimeReader.query( query ) ) );
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
            };
        }
    }
}
