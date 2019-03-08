/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ExtendWith( EphemeralFileSystemExtension.class )
public class DataCollectorObfuscationIT extends ProcedureInteractionTestBase<EnterpriseLoginContext>
{
    @Inject
    EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, () -> new UncloseableDelegatingFileSystemAbstraction( fileSystemRule.get() ) );
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
    void shouldObfuscatePasswordInDbStatsRetrieve() throws Exception
    {
        String secret = "abc123";
        String sillySecret = ".changePassword(\\'si\"lly\\')";
        String otherSillySecret = "other$silly";
        assertSuccess( adminSubject, "CALL db.stats.collect('QUERIES')", ResourceIterator::close );
        assertEmpty( adminSubject, format( "CALL dbms.security.changePassword('%s')", secret ) );
        assertEmpty( adminSubject, format( "CALL dbms.security.changeUserPassword('readSubject', '%s')", secret ) );
        assertEmpty( adminSubject, format( "CALL dbms.security.changeUserPassword('editorSubject', '%s', true)", secret ) );
        assertFail( adminSubject, format( "CALL dbms.security.changeUserPassword(null, '%s')", secret ), "" );
        assertFail( adminSubject, format( "CALL dbms.security.changeUserPassword('malformedUser, '%s')", secret ), "" );
        assertEmpty( adminSubject, format( "EXPLAIN CALL dbms.security.changePassword('%s')", secret ) );
        assertEmpty( adminSubject, format( "CALL dbms.security.changePassword('%s')", sillySecret ) );
        assertSuccess( adminSubject, format( "CALL dbms.security.changeUserPassword('writeSubject','%s') " +
                                             "CALL dbms.security.changeUserPassword('readSubject','%s') RETURN 1", sillySecret, otherSillySecret ), ResourceIterator::close );
        assertSuccess( adminSubject, "CALL db.stats.stop('QUERIES')", ResourceIterator::close );
        assertObfuscation( "CALL db.stats.retrieve('QUERIES')", secret, sillySecret, otherSillySecret );
        assertObfuscation( "CALL db.stats.retrieveAllAnonymized('graphToken')", secret, sillySecret, otherSillySecret );
    }

    private void assertObfuscation( String query, String... secrets )
    {
        assertSuccess( adminSubject, query,
                       itr -> {
                           List<String> queryTexts = itr.stream()
                                   .filter( s -> s.get("section").equals( "QUERIES" ) )
                                   .map( s -> (Map) s.get( "data" ) )
                                   .map( dataMap -> (String) dataMap.get( "query" ) )
                                   .collect( Collectors.toList() );

                           assertThat( queryTexts.size(), equalTo( 5 ) );
                           for ( String queryText : queryTexts )
                           {
                               assertThat( queryText, containsString( "dbms.security" ) );
                               for ( String secret : secrets )
                               {
                                   assertThat( queryText, not( containsString( secret ) ) );
                               }
                           }
                       } );
    }

    @Test
    void shouldObfuscateParameterPasswordInDbStatsRetrieve() throws Exception
    {
        Map<String,Object> secret = Collections.singletonMap( "password", "abc123" );
        Map<String,Object> secrets = new HashMap<>();
        secrets.put( "first", ".changePassword(silly)" );
        secrets.put( "second", ".other$silly" );

        assertSuccess( adminSubject, "CALL db.stats.collect('QUERIES')", ResourceIterator::close );
        assertEmpty( readSubject, "CALL dbms.changePassword($password)", secret );
        assertEmpty( writeSubject, "CALL dbms.changePassword({password})", secret );
        assertSuccess( adminSubject, "CALL dbms.security.changeUserPassword('writeSubject',$first) " +
                                     "CALL dbms.security.changeUserPassword('readSubject',$second) RETURN 1", secrets, ResourceIterator::close );
        assertSuccess( adminSubject, "CALL db.stats.stop('QUERIES')", ResourceIterator::close );
        assertSuccess( adminSubject, "CALL db.stats.retrieve('QUERIES')", itr -> {
            Stream<Map<String, Object>> parameterMapStream = itr.stream()
                    .map( s -> (Map) s.get( "data" ) )
                    .map( dataMap -> (List<Map>)dataMap.get( "invocations" ) )
                    .flatMap( Collection::stream )
                    .map( invocation -> (Map)invocation.get( "params" ) );

            List<Map<String,Object>> parameterMaps = parameterMapStream.collect( Collectors.<Map<String,Object>>toList() );

            assertThat( parameterMaps.size(), equalTo( 3 ) );
            for ( Map<String, Object> parameterMap : parameterMaps )
            {
                assertObfuscatedIfExists( parameterMap, "secret" );
                assertObfuscatedIfExists( parameterMap, "first" );
                assertObfuscatedIfExists( parameterMap, "second" );
            }
        } );
    }

    private void assertObfuscatedIfExists( Map<String,Object> map, String key )
    {
        Object value = map.get( key );
        if ( value != null )
        {
            assertThat( value, equalTo( "******" ) );
        }
    }
}
