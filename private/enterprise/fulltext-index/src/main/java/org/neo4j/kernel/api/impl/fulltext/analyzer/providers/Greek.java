/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.analyzer.providers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.helpers.Service;

@Service.Implementation( AnalyzerProvider.class )
public class Greek extends AnalyzerProvider
{
    public Greek()
    {
        super( "greek" );
    }

    @Override
    public Analyzer createAnalyzer()
    {
        return new GreekAnalyzer();
    }
}
