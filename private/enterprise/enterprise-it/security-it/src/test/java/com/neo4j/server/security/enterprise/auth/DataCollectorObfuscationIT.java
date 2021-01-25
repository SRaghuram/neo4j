/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.EphemeralFileSystemExtension;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@ExtendWith( EphemeralFileSystemExtension.class )
public class DataCollectorObfuscationIT extends ProcedureInteractionTestBase<EnterpriseLoginContext>
{
    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<Setting<?>,String> config, TestInfo testInfo )
    {
        return new EmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Test
    void shouldOmitDBMSQueriesInDbStatsRetrieve()
    {
        // given
        var oldSecret = "abc";
        String secret = "abc123";
        String sillySecret = ".changePassword(\\'si\"lly\\')";
        String otherSillySecret = "other$silly";
        assertSuccess( adminSubject, "CALL db.stats.stop('QUERIES')", ResourceIterator::close );
        assertSuccess( adminSubject, "CALL db.stats.clear('QUERIES')", ResourceIterator::close );
        assertNoDBMSQueries( "CALL db.stats.retrieve('QUERIES')" );

        // when
        assertSuccess( adminSubject, "CALL db.stats.collect('QUERIES')", ResourceIterator::close );
        assertSystemCommandSuccess( adminSubject, format( "ALTER CURRENT USER SET PASSWORD FROM '%s' TO '%s'", oldSecret, secret ) );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.changeUserPassword('readSubject', '%s')", secret ) );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.changeUserPassword('editorSubject', '%s', true)", secret ) );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.createUser('userA', '%s')", secret ) );
        assertSystemCommandSuccess( adminSubject, format( "CALL dbms.security.createUser('userB', '%s', true)", secret ) );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.suspendUser('userB')" );
        assertSystemCommandSuccess( adminSubject, "call dbms.security.listRoles()", ResourceIterator::close );
        assertSystemCommandSuccess( adminSubject, "CALL dbms.security.createRole('monkey')" );
        assertSuccess( adminSubject, "CALL dbms.killQuery('query-1234')", ResourceIterator::close );
        assertEmpty( adminSubject, "CALL tx.setMetaData({prop: 'itsAProp'})" );
        assertSystemCommandFail( adminSubject, format( "CALL dbms.security.changeUserPassword(null, '%s')", secret ), "" );
        assertSystemCommandFail( adminSubject, format( "CALL dbms.security.changeUserPassword('malformedUser, '%s')", secret ), "" );
        assertSystemCommandSuccess( adminSubject, format( "EXPLAIN CALL dbms.security.changePassword('%s')", secret ) );
        assertSystemCommandSuccess( adminSubject, format( "ALTER CURRENT USER SET PASSWORD FROM '%s' TO '%s'", secret, sillySecret ) );
        assertSuccess( adminSubject, "CALL db.stats.stop('QUERIES')", ResourceIterator::close );

        // then
        assertNoDBMSQueries( "CALL db.stats.retrieve('QUERIES')" );
        assertNoDBMSQueries( "CALL db.stats.retrieveAllAnonymized('graphToken')" );
    }

    private void assertNoDBMSQueries( String query )
    {
        assertSuccess( adminSubject, query,
                       itr -> {
                           List<String> queryTexts = itr.stream()
                                   .filter( s -> s.get("section").equals( "QUERIES" ) )
                                   .map( s -> (Map<String,String>) s.get( "data" ) )
                                   .map( dataMap -> dataMap.get( "query" ) )
                                   .filter( s -> s.toLowerCase().contains( "dbms" ))
                                   .collect( Collectors.toList() );

                           assertThat( queryTexts, containsInAnyOrder( ) );
                       } );
    }
}
