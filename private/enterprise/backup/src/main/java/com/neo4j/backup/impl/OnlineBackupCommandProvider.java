/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.Command.CommandType;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;

import static org.neo4j.cli.Command.CommandType.ONLINE_BACKUP;

@ServiceProvider
public class OnlineBackupCommandProvider implements CommandProvider<OnlineBackupCommand>
{
    @Override
    public OnlineBackupCommand createCommand( ExecutionContext ctx )
    {
        return new OnlineBackupCommand( ctx );
    }

    @Override
    public CommandType commandType()
    {
        return ONLINE_BACKUP;
    }
}
