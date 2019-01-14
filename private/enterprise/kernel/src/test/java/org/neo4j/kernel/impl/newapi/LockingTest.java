/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.LockingTestBase;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

public class LockingTest extends LockingTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport();
    }

    @Override
    protected LabelSchemaDescriptor labelDescriptor( int label, int... props )
    {
        return SchemaDescriptorFactory.forLabel( label, props );
    }
}
