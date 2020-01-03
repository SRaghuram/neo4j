/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.rule;

import com.neo4j.harness.EnterpriseNeo4jBuilders;

import java.io.File;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.harness.junit.rule.Neo4jRule;

/**
 * Enterprise Neo4j JUnit {@link org.junit.Rule rule}.
 * Allows easily start enterprise neo4j instance for testing purposes with various user-provided options and configurations.
 * <p>
 * By default it will try to start neo4j with embedded web server on random ports. Therefore it is necessary
 * for the test code to use {@link #httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 * <p>
 * In case if starting embedded web server is not desirable it can be fully disabled by using {@link #withDisabledServer()} configuration option.
 */
@PublicApi
public class EnterpriseNeo4jRule extends Neo4jRule
{
    public EnterpriseNeo4jRule()
    {
        super( EnterpriseNeo4jBuilders.newInProcessBuilder() );
    }

    public EnterpriseNeo4jRule( File workingDirectory )
    {
        super( EnterpriseNeo4jBuilders.newInProcessBuilder( workingDirectory ) );
    }
}
