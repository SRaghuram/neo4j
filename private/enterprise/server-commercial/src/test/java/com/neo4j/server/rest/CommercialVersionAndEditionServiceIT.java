/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.junit.Test;

import org.neo4j.kernel.internal.KernelData;
import org.neo4j.server.rest.management.VersionAndEditionService;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/*
Note that when running this test from within an IDE, the version field will be an empty string. This is because the
code that generates the version identifier is written by Maven as part of the build process(!). The tests will pass
both in the IDE (where the empty string will be correctly compared).
 */
public class CommercialVersionAndEditionServiceIT extends CommercialVersionIT
{

    @Test
    public void shouldReportEnterpriseEdition() throws Exception
    {
        // Given
        String releaseVersion = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( KernelData
                .class ).version().getReleaseVersion();

        // When
        HTTP.Response res =
                HTTP.GET( functionalTestHelper.managementUri() + "/" + VersionAndEditionService.SERVER_PATH );

        // Then
        assertEquals( 200, res.status() );
        assertThat( res.get( "edition" ).asText(), equalTo( "enterprise" ) );
        assertThat( res.get( "version" ).asText(), equalTo( releaseVersion ) );
    }
}
