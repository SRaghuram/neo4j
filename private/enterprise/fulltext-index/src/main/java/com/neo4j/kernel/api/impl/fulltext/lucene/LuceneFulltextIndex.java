/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Factory;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.storageengine.api.EntityType;

public class LuceneFulltextIndex extends AbstractLuceneIndex<FulltextIndexReader> implements Closeable
{
    private final AtomicLong transactionCounter;
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private final Collection<String> properties;
    private final TokenHolder propertyKeyTokenHolder;
    private final Factory<IndexWriterConfig> writerConfigFactory;
    private final File transactionsFolder;

    LuceneFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder, Factory<IndexWriterConfig> writerConfigFactory )
    {
        super( storage, partitionFactory, descriptor );
        this.analyzer = descriptor.analyzer();
        this.identifier = descriptor.getName();
        this.type = descriptor.schema().entityType();
        this.properties = descriptor.propertyNames();
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.writerConfigFactory = writerConfigFactory;
        transactionCounter = new AtomicLong();
        File indexFolder = storage.getIndexFolder();
        transactionsFolder = new File( indexFolder.getParent(), indexFolder.getName() + ".tx" );
    }

    @Override
    public void open() throws IOException
    {
        super.open();
        indexStorage.prepareFolder( transactionsFolder );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        indexStorage.cleanupFolder( transactionsFolder );
    }

    @Override
    public String toString()
    {
        return "LuceneFulltextIndex{" +
               "analyzer=" + analyzer.getClass().getSimpleName() +
               ", identifier='" + identifier + '\'' +
               ", type=" + type +
               ", properties=" + properties +
               ", descriptor=" + descriptor.userDescription( SchemaUtil.idTokenNameLookup ) +
               '}';
    }

    Directory createIndexTransactionDirectory() throws IOException
    {
        return indexStorage.openDirectory( new File( transactionsFolder, String.valueOf( transactionCounter.getAndIncrement() ) ) );
    }

    IndexWriterConfig createIndexWriterConfig()
    {
        return writerConfigFactory.newInstance();
    }

    String[] getPropertiesArray()
    {
        return properties.toArray( new String[0] );
    }

    Analyzer getAnalyzer()
    {
        return analyzer;
    }

    TokenHolder getPropertyKeyTokenHolder()
    {
        return propertyKeyTokenHolder;
    }

    @Override
    protected FulltextIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        SearcherReference searcher = new PartitionSearcherReference( singlePartition.acquireSearcher() );
        return new SimpleFulltextIndexReader( searcher, getPropertiesArray(), analyzer, propertyKeyTokenHolder );
    }

    @Override
    protected FulltextIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedFulltextIndexReader( searchers, getPropertiesArray(), analyzer, propertyKeyTokenHolder );
    }
}
