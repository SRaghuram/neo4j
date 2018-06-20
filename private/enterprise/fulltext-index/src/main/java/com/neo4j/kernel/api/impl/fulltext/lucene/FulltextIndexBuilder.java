/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriterConfig;

import org.neo4j.function.Factory;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.configuration.Config;

public class FulltextIndexBuilder extends AbstractLuceneIndexBuilder<FulltextIndexBuilder>
{
    private final FulltextIndexDescriptor descriptor;
    private final Analyzer analyzer;
    private Factory<IndexWriterConfig> writerConfigFactory = IndexWriterConfigs::standard;

    private FulltextIndexBuilder( FulltextIndexDescriptor descriptor, Config config, Analyzer analyzer )
    {
        super( config );
        this.descriptor = descriptor;
        this.analyzer = analyzer;
    }

    /**
     * Create new lucene fulltext index builder.
     *
     * @param descriptor The descriptor for this index
     * @return new FulltextIndexBuilder
     */
    public static FulltextIndexBuilder create( FulltextIndexDescriptor descriptor, Config config, Analyzer analyzer )
    {
        return new FulltextIndexBuilder( descriptor, config, analyzer );
    }

    /**
     * Specify {@link Factory} of lucene {@link IndexWriterConfig} to create {@link org.apache.lucene.index.IndexWriter}s.
     *
     * @param writerConfigFactory the supplier of writer configs
     * @return index builder
     */
    public FulltextIndexBuilder withWriterConfig( Factory<IndexWriterConfig> writerConfigFactory )
    {
        this.writerConfigFactory = writerConfigFactory;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public FulltextIndex build()
    {
        if ( isReadOnly() )
        {

            return new ReadOnlyFulltextIndex( storageBuilder.build(), new ReadOnlyIndexPartitionFactory(), analyzer, descriptor );
        }
        else
        {
            return new WritableFulltextIndex( storageBuilder.build(), new WritableIndexPartitionFactory( writerConfigFactory ), analyzer, descriptor );
        }
    }
}
