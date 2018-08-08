/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene.analyzer.providers;

import com.neo4j.kernel.api.impl.fulltext.lucene.analyzer.AnalyzerProvider;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;

import org.neo4j.helpers.Service;

@Service.Implementation( AnalyzerProvider.class )
public class Hindi extends AnalyzerProvider
{
    public Hindi()
    {
        super( "hindi" );
    }

    @Override
    public Analyzer createAnalyzer()
    {
        return new HindiAnalyzer();
    }
}
