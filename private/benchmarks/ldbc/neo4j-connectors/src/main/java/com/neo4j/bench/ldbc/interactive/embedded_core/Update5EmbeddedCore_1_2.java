/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate5;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.Calendar;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class Update5EmbeddedCore_1_2 extends Neo4jUpdate5<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate5AddForumMembership operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Calendar calendar = connection.calendar();
        TimeStampedRelationshipTypesCache timeStampedRelationshipTypes = connection.timeStampedRelationshipTypesCache();
        Node forum = Operators.findNode( connection.getTx(), Nodes.Forum, Forum.ID, operation.forumId() );
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        long joinDate = dateUtil.utcToFormat( operation.joinDate().getTime() );
        long joinDateAtResolution = dateUtil.formatToEncodedDateAtResolution( joinDate );
        RelationshipType hasMemberAtTime = timeStampedRelationshipTypes.hasMemberForDateAtResolution(
                joinDateAtResolution,
                dateUtil );
        Relationship membership = forum.createRelationshipTo( person, hasMemberAtTime );
        timeStampedRelationshipTypes.resizeHasMemberForNewDate( joinDateAtResolution, calendar, dateUtil );
        membership.setProperty( HasMember.JOIN_DATE, joinDate );
        return LdbcNoResult.INSTANCE;
    }
    /*
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate5AddForumMembership operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node forum = Operators.findNode( connection.db(), Nodes.Forum, Forum.ID, operation.forumId() );
        Node person = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.personId() );
        Relationship membership = forum.createRelationshipTo( person, Rels.HAS_MEMBER );
        membership.setProperty( HasMember.JOIN_DATE, dateUtil.utcToFormat( operation.joinDate().getTime() ) );
        return LdbcNoResult.INSTANCE;
    }
     */
}
