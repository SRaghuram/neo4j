/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class InfraCommand implements Runnable
{

    @Option( type = OptionType.GLOBAL,
             name = "--workspacePath",
             arity = 1,
             required = true )
    protected String workspacePath;

    @Option( type = OptionType.GLOBAL,
             name = "--awsKey",
             arity = 1,
             required = false )
    protected String awsKey;

    @Option( type = OptionType.GLOBAL,
             name = "--awsSecret",
             arity = 1,
             required = false )
    protected String awsSecret;

    @Option( type = OptionType.GLOBAL,
             name = "--awsRegion",
             arity = 1,
             required = false )
    protected String awsRegion = "eu-north-1";

}
