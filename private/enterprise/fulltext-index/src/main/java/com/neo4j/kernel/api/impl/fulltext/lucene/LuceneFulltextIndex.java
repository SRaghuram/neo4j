/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.analysis.Analyzer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

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
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private final Collection<String> properties;
    private final TokenHolder propertyKeyTokenHolder;

    LuceneFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder )
    {
        super( storage, partitionFactory, descriptor );
        this.analyzer = descriptor.analyzer();
        this.identifier = descriptor.getName();
        this.type = descriptor.schema().entityType();
        this.properties = descriptor.propertyNames();
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
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

    @Override
    protected FulltextIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        PartitionSearcher partitionSearcher = singlePartition.acquireSearcher();
        return new SimpleFulltextIndexReader( partitionSearcher, properties.toArray( new String[0] ), analyzer, propertyKeyTokenHolder );
    }

    @Override
    protected FulltextIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedFulltextIndexReader( searchers, properties.toArray( new String[0] ), analyzer, propertyKeyTokenHolder );
    }
}
