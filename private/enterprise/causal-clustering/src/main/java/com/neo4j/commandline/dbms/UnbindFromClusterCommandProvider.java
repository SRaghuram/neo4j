/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.cli.CommandProvider;
import org.neo4j.cli.ExecutionContext;

@ServiceProvider
public class UnbindFromClusterCommandProvider implements CommandProvider<UnbindFromClusterCommand>
{
    @Override
    public UnbindFromClusterCommand createCommand( ExecutionContext ctx )
    {
        return new UnbindFromClusterCommand( ctx );
    }
}
