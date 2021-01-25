/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.integration;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.importer.Scenario;
import org.junit.jupiter.api.Disabled;

@Disabled
public class EmbeddedCypherSnbBiExecutionTest extends SnbBiExecutionTest
{
    @Override
    Scenario buildValidationData() throws DbException
    {
        return Scenario.randomBi();
    }
}
