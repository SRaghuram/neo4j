/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.upgrade;

import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;

/**
 * Runs the store upgrader tests from older versions, migrating to the current enterprise version.
 */
public class StandardToEnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    public StandardToEnterpriseStoreUpgraderTest( RecordFormats recordFormats )
    {
        super( recordFormats );
    }

    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Override
    protected String getRecordFormatsName()
    {
        return HighLimit.NAME;
    }
}
