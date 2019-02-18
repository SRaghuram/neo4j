/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.configuration.LegacySslPolicyConfig;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.withBaseUri;

public class BatchEndpointIT
{
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withConfig( LegacySslPolicyConfig.certificates_directory,
                    getRelativePath( getSharedTestTemporaryFolder(), LegacySslPolicyConfig.certificates_directory ) )
            .withConfig( GraphDatabaseSettings.logs_directory,
                    getRelativePath( getSharedTestTemporaryFolder(), GraphDatabaseSettings.logs_directory ) )
            .withConfig( ServerSettings.http_logging_enabled, "true" )
            .withConfig( GraphDatabaseSettings.auth_enabled, "false" )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    @Test
    public void requestsShouldNotFailWhenHttpLoggingIsOn()
    {
        // Given
        String body = "[" +
                      "{'method': 'POST', 'to': '/node', 'body': {'age': 1}, 'id': 1} ]";

        // When
        Response response = withBaseUri( neo4j.httpURI() )
                .withHeaders( "Content-Type", "application/json" )
                .POST( "db/data/batch", quotedJson( body ) );

        // Then
        assertEquals( 200, response.status() );
    }
}
