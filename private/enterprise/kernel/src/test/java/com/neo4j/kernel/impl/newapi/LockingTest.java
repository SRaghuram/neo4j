/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.storageengine.api.schema.LabelSchemaDescriptor;

public class LockingTest extends LockingTestBase<EnterpriseWriteTestSupport>
{
    @Override
    public EnterpriseWriteTestSupport newTestSupport()
    {
        return new EnterpriseWriteTestSupport();
    }

    @Override
    protected LabelSchemaDescriptor labelDescriptor( int label, int... props )
    {
        return SchemaDescriptorFactory.forLabel( label, props );
    }
}
