/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.index.IndexWriterConfig;

import org.neo4j.function.Factory;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.configuration.Config;

public class FulltextIndexBuilder extends AbstractLuceneIndexBuilder<FulltextIndexBuilder>
{
    private final FulltextIndexDescriptor descriptor;
    private boolean populating;

    private FulltextIndexBuilder( FulltextIndexDescriptor descriptor, Config config )
    {
        super( config );
        this.descriptor = descriptor;
    }

    /**
     * Create new lucene fulltext index builder.
     *
     * @param descriptor The descriptor for this index
     * @return new FulltextIndexBuilder
     */
    public static FulltextIndexBuilder create( FulltextIndexDescriptor descriptor, Config config )
    {
        return new FulltextIndexBuilder( descriptor, config );
    }

    /**
     * Whether to create the index in a {@link IndexWriterConfigs#population() populating} mode, if {@code true}, or
     * in a {@link IndexWriterConfigs#standard() standard} mode, if {@code false}.
     * @param isPopulating {@code true} if the index should be created in a populating mode.
     * @return this index builder.
     */
    public FulltextIndexBuilder withPopulatingMode( boolean isPopulating )
    {
        this.populating = isPopulating;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public DatabaseIndex<FulltextIndexReader> build()
    {
        if ( isReadOnly() )
        {
            return new ReadOnlyFulltextIndex( storageBuilder.build(), new ReadOnlyIndexPartitionFactory(), descriptor );
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
            return new WritableFulltextIndex( storageBuilder.build(), new WritableIndexPartitionFactory( writerConfigFactory ), descriptor );
        }
    }
}
