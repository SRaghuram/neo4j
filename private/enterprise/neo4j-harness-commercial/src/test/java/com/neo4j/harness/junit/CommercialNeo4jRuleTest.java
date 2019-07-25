/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit;

import com.neo4j.harness.extensionpackage.MyEnterpriseUnmanagedExtension;
import com.neo4j.harness.junit.rule.CommercialNeo4jRule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.configuration.GraphDatabaseSettings.legacy_certificates_directory;
import static org.neo4j.server.ServerTestUtils.getRelativePath;

public class CommercialNeo4jRuleTest
{
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public Neo4jRule neo4j = new CommercialNeo4jRule()
            .withConfig( legacy_certificates_directory, getRelativePath( testDirectory.storeDir(), legacy_certificates_directory ) )
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
        assertThat( response.status(), equalTo( 234 ) );
    }

    @Test
    public void testPropertyExistenceConstraintCanBeCreated()
    {
        // Given running enterprise server
        String createConstraintUri = neo4j.httpURI().resolve( "test/myExtension/createConstraint" ).toString();

        // When I run this server

        // Then constraint should be created
        HTTP.Response response = HTTP.GET( createConstraintUri );
        assertThat( response.status(), equalTo( HttpStatus.CREATED_201 ) );
    }
}
