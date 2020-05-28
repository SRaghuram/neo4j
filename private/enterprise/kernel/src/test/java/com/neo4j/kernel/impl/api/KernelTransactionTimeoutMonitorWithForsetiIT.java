/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.api.KernelTransactionTimeoutMonitorIT;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import static com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLocksFactory.KEY;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.lock_manager;

class KernelTransactionTimeoutMonitorWithForsetiIT extends KernelTransactionTimeoutMonitorIT
{
    @ExtensionCallback
    protected void configure( TestDatabaseManagementServiceBuilder builder )
    {
        super.configure( builder );
        builder.setConfig( lock_manager, KEY );
    }

}
