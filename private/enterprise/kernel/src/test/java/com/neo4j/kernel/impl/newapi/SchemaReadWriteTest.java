/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.SchemaReadWriteTestBase;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.storageengine.api.schema.LabelSchemaDescriptor;
import org.neo4j.storageengine.api.schema.RelationTypeSchemaDescriptor;

import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;

public class SchemaReadWriteTest extends SchemaReadWriteTestBase<EnterpriseWriteTestSupport>
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

    @Override
    protected RelationTypeSchemaDescriptor typeDescriptor( int label, int...props )
    {
        return forRelType( label, props );
    }
}
