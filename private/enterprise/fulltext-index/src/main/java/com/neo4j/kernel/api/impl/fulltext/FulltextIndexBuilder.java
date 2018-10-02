/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.index.IndexWriterConfig;

import org.neo4j.function.Factory;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.TokenHolder;

public class FulltextIndexBuilder extends AbstractLuceneIndexBuilder<FulltextIndexBuilder>
{
    private final FulltextIndexDescriptor descriptor;
    private final TokenHolder propertyKeyTokenHolder;
    private boolean populating;
    private IndexUpdateSink indexUpdateSink = NullIndexUpdateSink.INSTANCE;

    private FulltextIndexBuilder( FulltextIndexDescriptor descriptor, Config config, TokenHolder propertyKeyTokenHolder )
    {
        super( config );
        this.descriptor = descriptor;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
    }

    /**
     * Create new lucene fulltext index builder.
     *
     * @param descriptor The descriptor for this index
     * @param propertyKeyTokenHolder A token holder used to look up property key token names by id.
     * @return new FulltextIndexBuilder
     */
    public static FulltextIndexBuilder create( FulltextIndexDescriptor descriptor, Config config, TokenHolder propertyKeyTokenHolder )
    {
        return new FulltextIndexBuilder( descriptor, config, propertyKeyTokenHolder );
    }

    /**
     * Whether to create the index in a {@link IndexWriterConfigs#population() populating} mode, if {@code true}, or
     * in a {@link IndexWriterConfigs#standard() standard} mode, if {@code false}.
     *
     * @param isPopulating {@code true} if the index should be created in a populating mode.
     * @return this index builder.
     */
    FulltextIndexBuilder withPopulatingMode( boolean isPopulating )
    {
        this.populating = isPopulating;
        return this;
    }

    FulltextIndexBuilder withIndexUpdateSink( IndexUpdateSink indexUpdateSink )
    {
        this.indexUpdateSink = indexUpdateSink;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public DatabaseFulltextIndex build()
    {
        if ( isReadOnly() )
        {
            final ReadOnlyIndexPartitionFactory partitionFactory = new ReadOnlyIndexPartitionFactory();
            LuceneFulltextIndex fulltextIndex =
                    new LuceneFulltextIndex( storageBuilder.build(), partitionFactory, descriptor, propertyKeyTokenHolder );
            return new ReadOnlyFulltextIndex( fulltextIndex );
        }
        else
        {
            Factory<IndexWriterConfig> writerConfigFactory;
            if ( populating )
            {
                writerConfigFactory = () -> IndexWriterConfigs.population( descriptor.analyzer() );
            }
            else
            {
                writerConfigFactory = () -> IndexWriterConfigs.standard( descriptor.analyzer() );
            }
            WritableIndexPartitionFactory partitionFactory = new WritableIndexPartitionFactory( writerConfigFactory );
            LuceneFulltextIndex fulltextIndex =
                    new LuceneFulltextIndex( storageBuilder.build(), partitionFactory, descriptor, propertyKeyTokenHolder );
            return new WritableFulltextIndex( indexUpdateSink, fulltextIndex );
        }
    }
}
