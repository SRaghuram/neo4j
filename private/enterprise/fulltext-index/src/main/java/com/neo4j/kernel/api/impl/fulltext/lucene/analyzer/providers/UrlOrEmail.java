/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene.analyzer.providers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.helpers.Service;

@Service.Implementation( AnalyzerProvider.class )
public class UrlOrEmail extends AnalyzerProvider
{
    public UrlOrEmail()
    {
        super( "url_or_email", "url", "email" );
    }

    @Override
    public Analyzer createAnalyzer()
    {
        return new UAX29URLEmailAnalyzer();
    }
}
