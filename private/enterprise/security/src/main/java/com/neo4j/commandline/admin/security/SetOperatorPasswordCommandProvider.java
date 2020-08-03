/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.admin.security;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.Command;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;

import static org.neo4j.cli.Command.CommandType.SET_INITIAL_PASSWORD;

@ServiceProvider
public class SetOperatorPasswordCommandProvider implements CommandProvider<SetOperatorPasswordCommand>
{
    @Override
    public SetOperatorPasswordCommand createCommand( ExecutionContext ctx )
    {
        return new SetOperatorPasswordCommand( ctx );
    }

    @Override
    public Command.CommandType commandType()
    {
        return SET_INITIAL_PASSWORD;
    }
}
