/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;

import java.util.NoSuchElementException;

import org.neo4j.helpers.Service;

public abstract class AnalyzerProvider extends Service
{
    /**
     * @param analyzerName The name of this analyzer provider, which will be used for analyzer settings values for identifying which implementation to use.
     */
    public AnalyzerProvider( String analyzerName, String... alternativeNames )
    {
        super( analyzerName, alternativeNames );
    }

    /**
     * Derive the analyzer name from the class name.
     * @see Service#Service()
     */
    public AnalyzerProvider()
    {
        super();
    }

    public static AnalyzerProvider getProviderByName( String analyzerName ) throws NoSuchElementException
    {
        return Service.load( AnalyzerProvider.class, analyzerName );
    }

    public abstract Analyzer createAnalyzer();
}
