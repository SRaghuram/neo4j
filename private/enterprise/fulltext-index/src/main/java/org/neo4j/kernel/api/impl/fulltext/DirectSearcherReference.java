/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.search.IndexSearcher;

import java.io.Closeable;
import java.io.IOException;

class DirectSearcherReference implements SearcherReference
{
    private final IndexSearcher searcher;
    private final Closeable resource;

    DirectSearcherReference( IndexSearcher searcher, Closeable resource )
    {
        this.searcher = searcher;
        this.resource = resource;
    }

    @Override
    public void close() throws IOException
    {
        resource.close();
    }

    @Override
    public IndexSearcher getIndexSearcher()
    {
        return searcher;
    }
}
