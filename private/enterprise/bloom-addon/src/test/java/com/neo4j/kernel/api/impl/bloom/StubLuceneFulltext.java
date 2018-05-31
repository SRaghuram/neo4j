/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import com.neo4j.kernel.api.impl.bloom.surface.BloomKernelExtensionFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import static java.util.Collections.singletonList;

public class StubLuceneFulltext extends LuceneFulltext
{
    StubLuceneFulltext()
    {
        super( null, null, singletonList( "props" ), new StandardAnalyzer(), BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES );
    }
}
