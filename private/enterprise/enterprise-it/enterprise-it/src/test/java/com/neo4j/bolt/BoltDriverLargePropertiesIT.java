/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.configuration.OnlineBackupSettings;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.driver.Values.parameters;

@RunWith( Parameterized.class )
public class BoltDriverLargePropertiesIT
{
    @ClassRule
    public static final Neo4jRule db = new Neo4jRule()
            .withConfig( OnlineBackupSettings.online_backup_enabled, false );

    private static Driver driver;

    @Parameter
    public int size;

    @BeforeClass
    public static void setUp() throws Exception
    {
        driver = graphDatabaseDriver( db.boltURI() );
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Parameters( name = "{0}" )
    public static List<Object> arraySizes()
    {
        return Arrays.asList( 1, 2, 3, 10, 999, 4_295, 10_001, 55_155, 100_000 );
    }

    @Test
    public void shouldSendAndReceiveString()
    {
        String originalValue = RandomStringUtils.randomAlphanumeric( size );
        Object receivedValue = sendAndReceive( originalValue );
        assertEquals( originalValue, receivedValue );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldSendAndReceiveLongArray()
    {
        List<Long> originalLongs = ThreadLocalRandom.current().longs( size ).boxed().collect( toList() );
        List<Long> receivedLongs = (List<Long>) sendAndReceive( originalLongs );
        assertEquals( originalLongs, receivedLongs );
    }

    private static Object sendAndReceive( Object value )
    {
        try ( Session session = driver.session() )
        {
            Result result = session.run( "RETURN $value", parameters( "value", value ) );
            Record record = result.single();
            return record.get( 0 ).asObject();
        }
    }
}
