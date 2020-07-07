/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.restore;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;

import static org.neo4j.cli.Command.CommandType;
import static org.neo4j.cli.Command.CommandType.RESTORE_DB;

@ServiceProvider
public class RestoreDatabaseCliProvider implements CommandProvider<RestoreDatabaseCli>
{
    @Override
    public RestoreDatabaseCli createCommand( ExecutionContext ctx )
    {
        return new RestoreDatabaseCli( ctx );
    }

    @Override
    public CommandType commandType()
    {
        return RESTORE_DB;
    }
}
