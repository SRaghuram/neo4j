/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.EntityType;

public interface FulltextAdapter
{
    String FIELD_ENTITY_ID = "__neo4j__lucene__fulltext__addon__internal__id__";

    Stream<String> propertyKeyStrings( IndexReference descriptor );

    SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, Optional<String> analyzerOverride,
                                String... properties );

    ScoreEntityIterator query( String indexName, String queryString ) throws IOException, IndexNotFoundKernelException, ParseException;
}
