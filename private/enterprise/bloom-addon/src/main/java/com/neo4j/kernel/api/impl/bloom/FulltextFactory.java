/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriterConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.function.Factory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

/**
 * Used for creating {@link LuceneFulltext} and registering those to a {@link FulltextProvider}.
 */
class FulltextFactory
{
    public static final String INDEX_DIR = "bloom_fts";
    private final FileSystemAbstraction fileSystem;
    private final WritableIndexPartitionFactory partitionFactory;
    private final File indexDir;
    private final Analyzer analyzer;

    /**
     * Creates a factory for the specified location and analyzer.
     *
     * @param fileSystem The filesystem to use.
     * @param storeDir Store directory of the database.
     * @param analyzerClassName The Lucene analyzer to use for the {@link LuceneFulltext} created by this factory.
     * @throws IOException
     */
    FulltextFactory( FileSystemAbstraction fileSystem, File storeDir, String analyzerClassName )
    {
        this.analyzer = getAnalyzer( analyzerClassName );
        this.fileSystem = fileSystem;
        Factory<IndexWriterConfig> indexWriterConfigFactory = () -> IndexWriterConfigs.standard( analyzer );
        partitionFactory = new WritableIndexPartitionFactory( indexWriterConfigFactory );
        indexDir = new File( storeDir, INDEX_DIR );
    }

    private Analyzer getAnalyzer( String analyzerClassName )
    {
        Analyzer analyzer;
        try
        {
            Class configuredAnalyzer = Class.forName( analyzerClassName );
            analyzer = (Analyzer) configuredAnalyzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create the configured analyzer", e );
        }
        return analyzer;
    }

    LuceneFulltext createFulltextIndex( String identifier, int side, FulltextIndexType type, List<String> properties )
    {
        File indexRootFolder = new File( indexDir, identifier + side );
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexFolder( indexRootFolder );
        PartitionedIndexStorage storage = storageBuilder.build();
        return new LuceneFulltext( storage, partitionFactory, properties, analyzer, identifier, type );
    }

    LuceneFulltext openFulltextIndex( String identifier, int side, FulltextIndexType type ) throws IOException
    {
        File indexRootFolder = new File( indexDir, identifier + side );
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexFolder( indexRootFolder );
        PartitionedIndexStorage storage = storageBuilder.build();
        return new LuceneFulltext( storage, partitionFactory, analyzer, identifier, type );
    }
}
