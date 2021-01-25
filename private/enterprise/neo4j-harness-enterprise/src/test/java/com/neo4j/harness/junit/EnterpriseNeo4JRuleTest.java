/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.harness.extensionpackage.MyEnterpriseUnmanagedExtension;
import com.neo4j.harness.junit.rule.EnterpriseNeo4jRule;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;

public class EnterpriseNeo4JRuleTest
{
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withUnmanagedExtension( "/test", MyEnterpriseUnmanagedExtension.class )
            .withConfig( OnlineBackupSettings.online_backup_enabled, false );

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldExtensionWork()
    {
        // Given running enterprise server
        String doSomethingUri = neo4j.httpURI().resolve( "test/myExtension/doSomething" ).toString();

        // When I run this test

        // Then
        HTTP.Response response = HTTP.GET( doSomethingUri );
        assertThat( response.status() ).isEqualTo( 234 );
    }

    @Test
    public void testPropertyExistenceConstraintCanBeCreated()
    {
        // Given running enterprise server
        String createConstraintUri = neo4j.httpURI().resolve( "test/myExtension/createConstraint" ).toString();

        // When I run this server

        // Then constraint should be created
        HTTP.Response response = HTTP.GET( createConstraintUri );
        assertThat( response.status() ).isEqualTo( HttpStatus.CREATED_201 );
    }
}
