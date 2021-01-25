/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;

import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;

/**
 * Runs the store upgrader tests from older versions, migrating to the current enterprise version.
 */
public class StandardToEnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    @Override
    protected String getRecordFormatsName()
    {
        return HighLimit.NAME;
    }
}
