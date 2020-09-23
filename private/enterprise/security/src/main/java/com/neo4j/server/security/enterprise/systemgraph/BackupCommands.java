/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import java.util.List;

public class BackupCommands
{
    public final List<String> roleSetup;
    public final List<String> userSetup;

    public BackupCommands( List<String> roleSetup, List<String> userSetup )
    {
        this.roleSetup = roleSetup;
        this.userSetup = userSetup;
    }
}
