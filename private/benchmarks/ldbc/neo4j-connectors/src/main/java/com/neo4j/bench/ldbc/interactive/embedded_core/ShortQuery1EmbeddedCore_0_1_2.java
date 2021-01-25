/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery1;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

public class ShortQuery1EmbeddedCore_0_1_2 extends Neo4jShortQuery1<Neo4jConnectionState>
{
    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.FIRST_NAME,
            Person.LAST_NAME,
            Person.BIRTHDAY,
            Person.LOCATION_IP,
            Person.BROWSER_USED,
            Person.GENDER,
            Person.CREATION_DATE};

    @Override
    public LdbcShortQuery1PersonProfileResult execute( Neo4jConnectionState connection,
            LdbcShortQuery1PersonProfile operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node city = person.getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING ).getEndNode();
        Map<String,Object> personProperties = person.getProperties( PERSON_PROPERTIES );
        return new LdbcShortQuery1PersonProfileResult(
                (String) personProperties.get( Person.FIRST_NAME ),
                (String) personProperties.get( Person.LAST_NAME ),
                dateUtil.formatToUtc( (long) personProperties.get( Person.BIRTHDAY ) ),
                (String) personProperties.get( Person.LOCATION_IP ),
                (String) personProperties.get( Person.BROWSER_USED ),
                (long) city.getProperty( Place.ID ),
                (String) personProperties.get( Person.GENDER ),
                dateUtil.formatToUtc( (long) personProperties.get( Person.CREATION_DATE ) )
        );
    }
}
