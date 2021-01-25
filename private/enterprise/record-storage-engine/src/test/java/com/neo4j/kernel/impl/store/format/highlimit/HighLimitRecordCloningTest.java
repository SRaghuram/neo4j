/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.AbstractRecordCloningTest;
import org.neo4j.kernel.impl.store.format.RecordFormats;

public class HighLimitRecordCloningTest extends AbstractRecordCloningTest
{
    @Override
    protected RecordFormats formats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Override
    protected int entityBits()
    {
        return 50;
    }

    @Override
    protected int propertyBits()
    {
        return 50;
    }
}
