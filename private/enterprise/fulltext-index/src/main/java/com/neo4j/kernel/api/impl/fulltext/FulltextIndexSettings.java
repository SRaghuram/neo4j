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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

class FulltextIndexSettings
{
    static final String INDEX_SETTINGS_FILE = "fulltext-index.properties";
    static final String SETTING_ANALYZER = "analyzer";
    static final String SETTING_EVENTUALLY_CONSISTENT = "eventually_consistent";

    static FulltextIndexDescriptor readOrInitialiseDescriptor( StoreIndexDescriptor descriptor, String defaultAnalyzerClassName,
            TokenHolder propertyKeyTokenHolder, PartitionedIndexStorage indexStorage, FileSystemAbstraction fileSystem )
    {
        Properties settings = new Properties();
        if ( descriptor.schema() instanceof FulltextSchemaDescriptor )
        {
            FulltextSchemaDescriptor schema = (FulltextSchemaDescriptor) descriptor.schema();
            settings.putAll( schema.getSettings() );
        }
        loadPersistedSettings( settings, indexStorage, fileSystem );
        boolean eventuallyConsistent = Boolean.parseBoolean( settings.getProperty( SETTING_EVENTUALLY_CONSISTENT ) );
        String analyzerClassName = settings.getProperty( SETTING_ANALYZER, defaultAnalyzerClassName );
        Analyzer analyzer = createAnalyzer( analyzerClassName );
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
        return new FulltextIndexDescriptor( descriptor, propertyNames, analyzer, eventuallyConsistent );
    }

    private static void loadPersistedSettings( Properties settings, PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_SETTINGS_FILE );
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

    static void saveFulltextIndexSettings( FulltextIndexDescriptor descriptor, PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
            throws IOException
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_SETTINGS_FILE );
        Properties settings = new Properties();
        settings.getProperty( SETTING_EVENTUALLY_CONSISTENT, Boolean.toString( descriptor.isEventuallyConsistent() ) );
        settings.setProperty( SETTING_ANALYZER, descriptor.analyzer().getClass().getName() );
        settings.setProperty( "_propertyNames", descriptor.propertyNames().toString() );
        settings.setProperty( "_propertyIds", Arrays.toString( descriptor.properties() ) );
        settings.setProperty( "_name", descriptor.name() );
        settings.setProperty( "_schema_entityType", descriptor.schema().entityType().name() );
        settings.setProperty( "_schema_entityTokenIds", Arrays.toString( descriptor.schema().getEntityTokenIds() ) );
        try ( StoreChannel channel = fs.create( settingsFile );
                Writer writer = fs.openAsWriter( settingsFile, StandardCharsets.UTF_8, false ) )
        {
            settings.store( writer, "Auto-generated file. Do not modify!" );
            writer.flush();
            channel.force( true );
        }
    }
}
