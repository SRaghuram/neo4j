/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.index.schema;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.index.schema.IndexCreateIT;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class IndexCreateEnterpriseIT extends IndexCreateIT
{
    private static final IndexCreator NODE_KEY_CREATOR =
            ( schemaWrite, schema, provider, name ) -> schemaWrite.nodeKeyConstraintCreate(
                    IndexPrototype.uniqueForSchema( schema, schemaWrite.indexProviderByName( provider ) ).withName( name ) );

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }

    @Test
    void shouldCreateNodeKeyConstraintWithSpecificExistingProviderName() throws KernelException
    {
        shouldCreateWithSpecificExistingProviderName( NODE_KEY_CREATOR );
    }

    @Test
    void shouldFailCreateNodeKeyWithNonExistentProviderName() throws KernelException
    {
        shouldFailWithNonExistentProviderName( NODE_KEY_CREATOR );
    }
}
