/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.extension;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.harness.junit.extension.Neo4jExtensionBuilder;

import static com.neo4j.harness.EnterpriseNeo4jBuilders.newInProcessBuilder;

/**
 * Enterprise Neo4j JUnit 5 Extension.
 * Allows easily start neo4j instance for testing purposes with junit 5 with various user-provided options and configurations.
 * Can be registered declaratively with {@link ExtendWith} or programmatically using {@link RegisterExtension}.
 * <p>
 * By default it will try to start neo4j with embedded web server on random ports.
 * In case if more advance configuration is required please use {@link RegisterExtension programmatical extension registration} and configure
 * desired Neo4j behaviour using available options.
 * <p>
 * Please note that neo4j server uses dynamic ports and it is necessary
 * for the test code to use {@link Neo4j#httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 * <p>
 * In case if starting embedded web server is not desirable it can be fully disabled by using {@link Neo4jExtensionBuilder#withDisabledServer()}.
 * <p>
 * Usage example:
 * <pre>
 *  <code>
 *    {@literal @}ExtendWith( EnterpriseNeo4jExtension.class )
 *     class TestExample {
 *            {@literal @}Test
 *             void testExample( Neo4j neo4j, GraphDatabaseService databaseService )
 *             {
 *                 // test code
 *             }
 *   }
 *
 *  </code>
 * </pre>
 */
@PublicApi
public class EnterpriseNeo4jExtension extends Neo4jExtension
{
    public static Neo4jExtensionBuilder builder()
    {
        return new EnterpriseNeo4jExtensionBuilder();
    }

    public EnterpriseNeo4jExtension()
    {
        super( newInProcessBuilder() );
    }
}
