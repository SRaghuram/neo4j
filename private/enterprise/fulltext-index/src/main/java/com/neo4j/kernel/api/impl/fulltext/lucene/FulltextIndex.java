/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;

public interface FulltextIndex extends DatabaseIndex<FulltextIndexReader>
{
    @Override
    FulltextIndexReader getIndexReader() throws IOException;
}
