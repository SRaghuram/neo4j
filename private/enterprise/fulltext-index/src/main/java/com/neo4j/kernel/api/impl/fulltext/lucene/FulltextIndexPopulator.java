/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;

public class FulltextIndexPopulator extends LuceneIndexPopulator<FulltextIndex>
{
    private final FulltextIndexDescriptor descriptor;
    private final ThrowingAction<IOException> descriptorCreateAction;

    public FulltextIndexPopulator( FulltextIndexDescriptor descriptor, FulltextIndex luceneFulltext,
                                  ThrowingAction<IOException> descriptorCreateAction )
    {
        super( luceneFulltext );
        this.descriptor = descriptor;
        this.descriptorCreateAction = descriptorCreateAction;
    }

    @Override
    public void create() throws IOException
    {
        super.create();
        descriptorCreateAction.apply();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates ) throws IOException
    {
        luceneIndex.getIndexWriter().addDocuments( updates.size(), () -> updates.stream().map( this::updateAsDocument ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor propertyAccessor )
    {
        //Fulltext index does not care about constraints.
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new PopulatingFulltextIndexUpdater();
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        //Index sampling is not our thing, really.
    }

    @Override
    public IndexSample sampleResult()
    {
        return new IndexSample();
    }

    private Document updateAsDocument( IndexEntryUpdate<?> update )
    {
        return LuceneFulltextDocumentStructure.documentRepresentingProperties( update.getEntityId(), descriptor.propertyNames(), update.values() );
    }

    private class PopulatingFulltextIndexUpdater implements IndexUpdater
    {
        @Override
        public void process( IndexEntryUpdate<?> update ) throws IOException
        {
            assert update.indexKey().schema().equals( descriptor.schema() );

            switch ( update.updateMode() )
            {
            case ADDED:
                long nodeId = update.getEntityId();
                luceneIndex.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ),
                        LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), update.values() ) );

            case CHANGED:
                long nodeId1 = update.getEntityId();
                luceneIndex.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId1 ),
                        LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId1, descriptor.propertyNames(), update.values() ) );
                break;
            case REMOVED:
                luceneIndex.getIndexWriter().deleteDocuments( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( update.getEntityId() ) );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close()
        {
        }
    }
}
