/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.batchinsert;

import org.junit.jupiter.api.Test;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.neo4j.batchinsert.BatchInserters.inserter;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;

@Neo4jLayoutExtension
class BatchInsertersIT
{
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldStartBatchInserterWithRealIndexProvider() throws Exception
    {
        BatchInserter inserter = inserter( databaseLayout, getConfig(), getExtensions() );
        inserter.shutdown();
    }

    private static Config getConfig()
    {
        return Config.defaults( default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
    }

    private static Iterable<ExtensionFactory<?>> getExtensions()
    {
        return Iterables.asIterable( new GenericNativeIndexProviderFactory() );
    }
}
