/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext.analyzer.providers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.helpers.Service;

@Service.Implementation( AnalyzerProvider.class )
public class Armenian extends AnalyzerProvider
{
    public Armenian()
    {
        super( "armenian" );
    }

    @Override
    public Analyzer createAnalyzer()
    {
        return new ArmenianAnalyzer();
    }
}
