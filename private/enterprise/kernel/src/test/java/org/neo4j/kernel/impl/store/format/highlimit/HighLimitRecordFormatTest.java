/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;

public class HighLimitRecordFormatTest extends AbstractRecordFormatTest
{
    public HighLimitRecordFormatTest()
    {
        super( HighLimit.RECORD_FORMATS, 50, 50 );
    }
}
