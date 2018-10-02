/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

interface SearcherReference
{
    void close() throws IOException;
    IndexSearcher getIndexSearcher();
}
