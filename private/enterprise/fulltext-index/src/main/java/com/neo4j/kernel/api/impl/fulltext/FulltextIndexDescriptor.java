/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

public class FulltextIndexDescriptor extends StoreIndexDescriptor
{
    private static final String INDEX_SETTINGS_FILE = "fulltext-index.properties";
    private static final String SETTING_ANALYZER = "analyzer";

    private final Set<String> propertyNames;
    private final Analyzer analyzer;

    FulltextIndexDescriptor( StoreIndexDescriptor descriptor,
                             String suggestedAnalyzerClassName,
                             TokenHolder propertyKeyTokenHolder,
                             PartitionedIndexStorage indexStorage,
                             FileSystemAbstraction fileSystem )
    {
        super( descriptor );
        if ( descriptor.schema() instanceof FulltextSchemaDescriptor )
        {
            FulltextSchemaDescriptor schema = (FulltextSchemaDescriptor) descriptor.schema();
            suggestedAnalyzerClassName = schema.getAnalyzerClassName();
        }
        String analyzerClassName =
                getPersistedAnalyzerClassName( indexStorage, fileSystem ).orElse( suggestedAnalyzerClassName );
        this.analyzer = createAnalyzer( analyzerClassName );
        Set<String> names = new HashSet<>();
        for ( int propertyKeyId : this.schema.getPropertyIds() )
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
        propertyNames = Collections.unmodifiableSet( names );
    }

    private Optional<String> getPersistedAnalyzerClassName(
            PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_SETTINGS_FILE );
        if ( !fs.fileExists( settingsFile ) )
        {
            return Optional.empty();
        }
        Properties settings = new Properties();
        try ( Reader reader = fs.openAsReader( settingsFile, StandardCharsets.UTF_8 ) )
        {
            settings.load( reader );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to read persisted fulltext index properties: " + settingsFile, e );
        }
        return Optional.ofNullable( settings.getProperty( SETTING_ANALYZER ) );
    }

    void saveIndexSettings( PartitionedIndexStorage indexStorage, FileSystemAbstraction fs ) throws IOException
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_SETTINGS_FILE );
        Properties settings = new Properties();
        settings.setProperty( SETTING_ANALYZER, analyzer.getClass().getName() );
        try ( StoreChannel channel = fs.create( settingsFile );
              Writer writer = fs.openAsWriter( settingsFile, StandardCharsets.UTF_8, false ) )
        {
            settings.store( writer, "Auto-generated file. Do not modify!" );
            writer.flush();
            channel.force( true );
        }
    }

    private static Analyzer createAnalyzer( String analyzerClassName )
    {
        Analyzer analyzer;
        try
        {
            Class configuredAnalyzer = Class.forName( analyzerClassName );
            analyzer = (Analyzer) configuredAnalyzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create fulltext analyzer: " + analyzerClassName, e );
        }
        return analyzer;
    }

    public Collection<String> propertyNames()
    {
        return propertyNames;
    }

    public Analyzer analyzer()
    {
        return analyzer;
    }
}
