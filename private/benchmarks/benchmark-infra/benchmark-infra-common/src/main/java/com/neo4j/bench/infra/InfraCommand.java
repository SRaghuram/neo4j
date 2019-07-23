/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;

public abstract class InfraCommand implements Runnable
{

    @Option( type = OptionType.GLOBAL,
             name = "--workspacePath",
             arity = 1 )
    @Required
    protected String workspacePath;

    @Option( type = OptionType.GLOBAL,
             name = "--awsKey",
             arity = 1 )
    protected String awsKey;

    @Option( type = OptionType.GLOBAL,
             name = "--awsSecret",
             arity = 1 )
    protected String awsSecret;

    @Option( type = OptionType.GLOBAL,
             name = "--awsRegion",
             arity = 1 )
    protected String awsRegion = "eu-north-1";

}
