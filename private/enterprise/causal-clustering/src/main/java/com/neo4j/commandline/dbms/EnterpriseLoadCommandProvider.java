/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.Command.CommandType;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.archive.Loader;

import static org.neo4j.cli.Command.CommandType.LOAD;

@ServiceProvider
public class EnterpriseLoadCommandProvider implements CommandProvider<EnterpriseLoadCommand>
{
    @Override
    public EnterpriseLoadCommand createCommand( ExecutionContext ctx )
    {
        return new EnterpriseLoadCommand( ctx, new Loader( ctx.err() ) );
    }

    @Override
    public CommandType commandType()
    {
        return LOAD;
    }

    @Override
    public int getPriority()
    {
        return 2;
    }
}
