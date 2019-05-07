/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.impl.newapi.SchemaReadWriteTestBase;

import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;

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
        return forLabel( label, props );
    }

    @Override
    protected LabelSchemaDescriptor labelDescriptorNoIndex( int label, int... props )
    {
        return forLabel( label, props );
    }

    @Override
    protected RelationTypeSchemaDescriptor typeDescriptorNoIndex( int label, int...props )
    {
        return forRelType( label, props );
    }
}
