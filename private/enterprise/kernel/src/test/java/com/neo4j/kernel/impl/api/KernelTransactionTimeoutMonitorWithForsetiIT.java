/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api;

import com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLocksFactory;

import org.neo4j.kernel.impl.api.KernelTransactionTimeoutMonitorIT;
import org.neo4j.test.rule.DbmsRule;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.lock_manager;

public class KernelTransactionTimeoutMonitorWithForsetiIT extends KernelTransactionTimeoutMonitorIT
{
    @Override
    protected DbmsRule createDatabaseRule()
    {
        return super.createDatabaseRule().withSetting( lock_manager, ForsetiLocksFactory.KEY );
    }
}
