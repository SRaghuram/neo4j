/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server;

import com.neo4j.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.BeforeEach;

import org.neo4j.server.startup.Neo4jCommandIT;

class EnterpriseNeo4jCommandIT extends Neo4jCommandIT
{
    @BeforeEach
    public void setUp() throws Exception
    {
        super.setUp();
        addConf( OnlineBackupSettings.online_backup_enabled, "false" );
    }

    @Override
    protected String getVersion()
    {
        return "Enterprise";
    }
}
