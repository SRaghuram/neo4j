/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import java.io.IOException;
import java.util.Collection;

public interface ReadOnlyFulltext extends AutoCloseable
{
    /**
     * Searches the fulltext index for any exact match of any of the given terms against any token in any of the indexed properties.
     *
     *
     * @param terms The terms to query for.
     * @param matchAll If true, only resluts that match all the given terms will be returned
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    ScoreEntityIterator query( Collection<String> terms, boolean matchAll );

    /**
     * Searches the fulltext index for any fuzzy match of any of the given terms against any token in any of the indexed properties.
     *
     *
     * @param terms The terms to query for.
     * @param matchAll If true, only resluts that match all the given terms will be returned
     * @return An iterator over the matching entityIDs, ordered by lucene scoring of the match.
     */
    ScoreEntityIterator fuzzyQuery( Collection<String> terms, boolean matchAll );

    @Override
    void close();

    FulltextIndexConfiguration getConfigurationDocument() throws IOException;
}
