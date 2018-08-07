/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.analyzer.AnalyzerProvider;
import org.apache.lucene.analysis.Analyzer;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

class FulltextIndexSettings
{
    private static final String INDEX_CONFIG_FILE = "fulltext-index.properties";
    static final String INDEX_CONFIG_ANALYZER = "analyzer";
    static final String INDEX_CONFIG_EVENTUALLY_CONSISTENT = "eventually_consistent";

    static FulltextIndexDescriptor readOrInitialiseDescriptor( StoreIndexDescriptor descriptor, String defaultAnalyzerName,
            TokenHolder propertyKeyTokenHolder, PartitionedIndexStorage indexStorage, FileSystemAbstraction fileSystem )
    {
        Properties indexConfiguration = new Properties();
        if ( descriptor.schema() instanceof FulltextSchemaDescriptor )
        {
            FulltextSchemaDescriptor schema = (FulltextSchemaDescriptor) descriptor.schema();
            indexConfiguration.putAll( schema.getIndexConfiguration() );
        }
        loadPersistedSettings( indexConfiguration, indexStorage, fileSystem );
        boolean eventuallyConsistent = Boolean.parseBoolean( indexConfiguration.getProperty( INDEX_CONFIG_EVENTUALLY_CONSISTENT ) );
        String analyzerName = indexConfiguration.getProperty( INDEX_CONFIG_ANALYZER, defaultAnalyzerName );
        Analyzer analyzer = createAnalyzer( analyzerName );
        Set<String> names = new HashSet<>();
        for ( int propertyKeyId : descriptor.schema().getPropertyIds() )
        {
            try
            {
                names.add( propertyKeyTokenHolder.getTokenById( propertyKeyId ).name() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new IllegalStateException( "Property key id not found.",
                        new PropertyKeyIdNotFoundKernelException( propertyKeyId, e ) );
            }
        }
        Set<String> propertyNames = Collections.unmodifiableSet( names );
        return new FulltextIndexDescriptor( descriptor, propertyNames, analyzer, analyzerName, eventuallyConsistent );
    }

    private static void loadPersistedSettings( Properties settings, PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_CONFIG_FILE );
        if ( fs.fileExists( settingsFile ) )
        {
            try ( Reader reader = fs.openAsReader( settingsFile, StandardCharsets.UTF_8 ) )
            {
                settings.load( reader );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Failed to read persisted fulltext index properties: " + settingsFile, e );
            }
        }
    }

    private static Analyzer createAnalyzer( String analyzerName )
    {
        try
        {
            AnalyzerProvider provider = AnalyzerProvider.getProviderByName( analyzerName );
            return provider.createAnalyzer();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create fulltext analyzer: " + analyzerName, e );
        }
    }

    static void saveFulltextIndexSettings( FulltextIndexDescriptor descriptor, PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
            throws IOException
    {
        File indexConfigFile = new File( indexStorage.getIndexFolder(), INDEX_CONFIG_FILE );
        Properties settings = new Properties();
        settings.getProperty( INDEX_CONFIG_EVENTUALLY_CONSISTENT, Boolean.toString( descriptor.isEventuallyConsistent() ) );
        settings.setProperty( INDEX_CONFIG_ANALYZER, descriptor.analyzerName() );
        settings.setProperty( "_propertyNames", descriptor.propertyNames().toString() );
        settings.setProperty( "_propertyIds", Arrays.toString( descriptor.properties() ) );
        settings.setProperty( "_name", descriptor.name() );
        settings.setProperty( "_schema_entityType", descriptor.schema().entityType().name() );
        settings.setProperty( "_schema_entityTokenIds", Arrays.toString( descriptor.schema().getEntityTokenIds() ) );
        try ( StoreChannel channel = fs.create( indexConfigFile );
                Writer writer = fs.openAsWriter( indexConfigFile, StandardCharsets.UTF_8, false ) )
        {
            settings.store( writer, "Auto-generated file. Do not modify!" );
            writer.flush();
            channel.force( true );
        }
    }
}
