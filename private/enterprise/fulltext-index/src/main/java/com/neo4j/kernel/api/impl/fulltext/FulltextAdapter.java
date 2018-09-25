/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.storageengine.api.EntityType;

public interface FulltextAdapter
{
    SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, Properties indexConfiguration, String... properties );

    ScoreEntityIterator query( KernelTransaction tx, String indexName, String queryString ) throws IOException, IndexNotFoundKernelException, ParseException;

    void awaitRefresh();

    Stream<String> listAvailableAnalyzers();
}
