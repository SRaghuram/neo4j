/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.index.schema;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.test.TestGraphDatabaseFactory;

public class IndexCreateEnterpriseIT extends IndexCreateIT
{
    private static final IndexCreator NODE_KEY_CREATOR = SchemaWrite::nodeKeyConstraintCreate;

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestCommercialGraphDatabaseFactory();
    }

    @Test
    public void shouldCreateNodeKeyConstraintWithSpecificExistingProviderName() throws KernelException
    {
        shouldCreateWithSpecificExistingProviderName( NODE_KEY_CREATOR );
    }

    @Test
    public void shouldFailCreateNodeKeyWithNonExistentProviderName() throws KernelException
    {
        shouldFailWithNonExistentProviderName( NODE_KEY_CREATOR );
    }
}
