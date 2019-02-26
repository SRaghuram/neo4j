/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.batchinsert;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.unsafe.batchinsert.BatchInserters.inserter;

@ExtendWith( TestDirectoryExtension.class )
class BatchInsertersIT
{

    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldStartBatchInserterWithRealIndexProvider() throws Exception
    {
        BatchInserter inserter = inserter( testDirectory.databaseDir(), getConfig(), getExtensions() );
        inserter.shutdown();
    }

    private static Map<String,String> getConfig()
    {
        return MapUtil.stringMap( default_schema_provider.name(), GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
    }

    private static Iterable<ExtensionFactory<?>> getExtensions()
    {
        return Iterables.asIterable( new GenericNativeIndexProviderFactory() );
    }
}
