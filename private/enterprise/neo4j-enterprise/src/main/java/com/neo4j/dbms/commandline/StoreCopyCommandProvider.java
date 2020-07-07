/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.Command;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;

import static org.neo4j.cli.Command.CommandType.STORE_COPY;

@ServiceProvider
public class StoreCopyCommandProvider implements CommandProvider<StoreCopyCommand>
{
    @Override
    public StoreCopyCommand createCommand( ExecutionContext ctx )
    {
        return new StoreCopyCommand( ctx );
    }

    @Override
    public Command.CommandType commandType()
    {
        return STORE_COPY;
    }
}
