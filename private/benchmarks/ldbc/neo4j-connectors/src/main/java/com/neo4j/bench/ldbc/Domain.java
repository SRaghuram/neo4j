/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.util.Tuple2;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class Domain
{
    public static Set<Tuple2<Label,String>> nodesToMandatory( Neo4jSchema neo4jSchema )
    {
        Set<Tuple2<Label,String>> toMandatory = new HashSet<>();
        if ( neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) )
        {
            toMandatory.add( new Tuple2<>( Nodes.Message, Message.ID ) );
            toMandatory.add( new Tuple2<>( Nodes.Message, Message.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Nodes.Message, Message.LOCATION_IP ) );
            toMandatory.add( new Tuple2<>( Nodes.Message, Message.BROWSER_USED ) );
            toMandatory.add( new Tuple2<>( Nodes.Message, Message.LENGTH ) );
            // TODO should this always exist? check with Arnau
//        labelPropertyPairsToIndex.add( new Tuple2<>( Nodes.Post, Post.LANGUAGE ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.ID ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.FIRST_NAME ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.LAST_NAME ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.GENDER ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.BIRTHDAY ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.LOCATION_IP ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.BROWSER_USED ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.LANGUAGES ) );
            toMandatory.add( new Tuple2<>( Nodes.Person, Person.EMAIL_ADDRESSES ) );
            toMandatory.add( new Tuple2<>( Nodes.Forum, Forum.ID ) );
            toMandatory.add( new Tuple2<>( Nodes.Forum, Forum.TITLE ) );
            toMandatory.add( new Tuple2<>( Nodes.Forum, Forum.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Nodes.Tag, Tag.ID ) );
            toMandatory.add( new Tuple2<>( Nodes.Tag, Tag.NAME ) );
            toMandatory.add( new Tuple2<>( Nodes.TagClass, TagClass.NAME ) );
            toMandatory.add( new Tuple2<>( Organisation.Type.Company, Organisation.ID ) );
            toMandatory.add( new Tuple2<>( Organisation.Type.Company, Organisation.NAME ) );
            toMandatory.add( new Tuple2<>( Organisation.Type.University, Organisation.ID ) );
            toMandatory.add( new Tuple2<>( Organisation.Type.University, Organisation.NAME ) );
        }
        return toMandatory;
    }

    public static Set<Tuple2<RelationshipType,String>> relationshipsToMandatory( Neo4jSchema neo4jSchema )
    {
        Set<Tuple2<RelationshipType,String>> toMandatory = new HashSet<>();
        if ( neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) )
        {
            toMandatory.add( new Tuple2<>( Rels.KNOWS, Knows.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Rels.STUDY_AT, StudiesAt.CLASS_YEAR ) );
            toMandatory.add( new Tuple2<>( Rels.WORKS_AT, WorksAt.WORK_FROM ) );
            toMandatory.add( new Tuple2<>( Rels.LIKES_COMMENT, Likes.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Rels.LIKES_POST, Likes.CREATION_DATE ) );
            toMandatory.add( new Tuple2<>( Rels.HAS_MEMBER, HasMember.JOIN_DATE ) );
        }
        return toMandatory;
    }

    public static Set<Tuple2<Label,String>> toUnique( Neo4jSchema neo4jSchema )
    {
        Set<Tuple2<Label,String>> toUnique = new HashSet<>();
        toUnique.add( new Tuple2<>( Nodes.Person, Person.ID ) );
        toUnique.add( new Tuple2<>( Place.Type.Country, Place.ID ) );
        toUnique.add( new Tuple2<>( Place.Type.Country, Place.NAME ) );
        toUnique.add( new Tuple2<>( Place.Type.City, Place.NAME ) );
        toUnique.add( new Tuple2<>( Nodes.Tag, Tag.ID ) );
        toUnique.add( new Tuple2<>( Nodes.Tag, Tag.NAME ) );
        toUnique.add( new Tuple2<>( Nodes.TagClass, TagClass.NAME ) );
        toUnique.add( new Tuple2<>( Nodes.Message, Message.ID ) );
        toUnique.add( new Tuple2<>( Nodes.Forum, Forum.ID ) );
        toUnique.add( new Tuple2<>( Nodes.GraphMetaData, GraphMetadata.ID ) );
        return toUnique;
    }

    public static Set<Tuple2<Label,String>> toIndex(
            Neo4jSchema neo4jSchema,
            Set<Tuple2<Label,String>> toUnique )
    {
        Set<Tuple2<Label,String>> toIndex = new HashSet<>();
        toIndex.add( new Tuple2<>( Nodes.Person, Person.ID ) );
        toIndex.add( new Tuple2<>( Nodes.Person, Person.FIRST_NAME ) );
        toIndex.add( new Tuple2<>( Place.Type.Country, Place.ID ) );
        toIndex.add( new Tuple2<>( Place.Type.Country, Place.NAME ) );
        toIndex.add( new Tuple2<>( Place.Type.City, Place.NAME ) );
        toIndex.add( new Tuple2<>( Nodes.Tag, Tag.ID ) );
        toIndex.add( new Tuple2<>( Nodes.Tag, Tag.NAME ) );
        toIndex.add( new Tuple2<>( Nodes.TagClass, TagClass.NAME ) );
        toIndex.add( new Tuple2<>( Nodes.Message, Message.ID ) );
        toIndex.add( new Tuple2<>( Nodes.Message, Message.CREATION_DATE ) );
        toIndex.add( new Tuple2<>( Nodes.Forum, Forum.ID ) );
        toIndex.add( new Tuple2<>( Nodes.GraphMetaData, GraphMetadata.ID ) );
        if ( neo4jSchema.equals( Neo4jSchema.NEO4J_REGULAR ) )
        {
            toIndex.add( new Tuple2<>( Nodes.Person, Person.LAST_NAME ) );
            toIndex.add( new Tuple2<>( Nodes.Person, Person.BIRTHDAY_MONTH ) );
            toIndex.add( new Tuple2<>( Nodes.Person, Person.BIRTHDAY_DAY_OF_MONTH ) );
        }
        // remove pairs that already have constraints, as constraints are backed by indexes anyway
        for ( Tuple2<Label,String> labelProperty : toUnique )
        {
            toIndex.remove( labelProperty );
        }
        return toIndex;
    }

    public enum Rels implements RelationshipType
    {
        STUDY_AT,
        REPLY_OF_COMMENT,
        REPLY_OF_POST,
        PERSON_IS_LOCATED_IN,
        ORGANISATION_IS_LOCATED_IN,
        POST_IS_LOCATED_IN,
        COMMENT_IS_LOCATED_IN,
        IS_PART_OF,
        KNOWS,
        HAS_MODERATOR,
        POST_HAS_CREATOR,
        COMMENT_HAS_CREATOR,
        WORKS_AT,
        HAS_INTEREST,
        LIKES_POST,
        LIKES_COMMENT,
        HAS_MEMBER,
        HAS_MEMBER_WITH_POSTS,
        CONTAINER_OF,
        FORUM_HAS_TAG,
        POST_HAS_TAG,
        COMMENT_HAS_TAG,
        HAS_TYPE,
        IS_SUBCLASS_OF
    }

    public enum Nodes implements Label
    {
        Message,
        Comment,
        Post,
        Person,
        Forum,
        Tag,
        TagClass,
        GraphMetaData
    }

    /*
     * Nodes
     */

    public static class Message
    {
        public static final String ID = "id";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String CONTENT = "content";
        public static final String LENGTH = "length";
    }

    public static class Post
    {
        public static final String IMAGE_FILE = "imageFile";
        public static final String LANGUAGE = "language";
    }

    public static class Person
    {
        public static final String ID = "id";
        public static final String FIRST_NAME = "firstName";
        public static final String LAST_NAME = "lastName";
        public static final String GENDER = "gender";
        public static final String BIRTHDAY = "birthday";
        public static final String BIRTHDAY_MONTH = "birthday_month";
        public static final String BIRTHDAY_DAY_OF_MONTH = "birthday_day";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String LANGUAGES = "languages";
        public static final String EMAIL_ADDRESSES = "email";
    }

    public static class Forum
    {
        public static final String ID = "id";
        public static final String TITLE = "title";
        public static final String CREATION_DATE = "creationDate";
    }

    public static class Tag
    {
        public static final String ID = "id";
        public static final String NAME = "name";
    }

    public static class TagClass
    {
        public static final String NAME = "name";
    }

    public static class Organisation
    {
        public enum Type implements Label
        {
            University,
            Company
        }

        public static final String ID = "id";
        public static final String NAME = "name";
    }

    public static class Place
    {
        public enum Type implements Label
        {
            Country,
            City,
            Continent
        }

        public static final String ID = "id";
        public static final String NAME = "name";
    }

    public static class GraphMetadata
    {
        public static final int ID_VALUE = 1;
        public static final String ID = "id";
        public static final String COMMENT_HAS_CREATOR_MIN_DATE = "commentHasCreatorMinDate";
        public static final String COMMENT_HAS_CREATOR_MAX_DATE = "commentHasCreatorMaxDate";
        public static final String POST_HAS_CREATOR_MIN_DATE = "postHasCreatorMinDate";
        public static final String POST_HAS_CREATOR_MAX_DATE = "postHasCreatorMaxDate";
        public static final String WORK_FROM_MIN_YEAR = "workFromMinYear";
        public static final String WORK_FROM_MAX_YEAR = "workFromMaxYear";
        public static final String COMMENT_IS_LOCATED_IN_MIN_DATE = "commentIsLocatedInMinDate";
        public static final String COMMENT_IS_LOCATED_IN_MAX_DATE = "commentIsLocatedInMaxDate";
        public static final String POST_IS_LOCATED_IN_MIN_DATE = "postIsLocatedInMinDate";
        public static final String POST_IS_LOCATED_IN_MAX_DATE = "postIsLocatedInMaxDate";
        public static final String HAS_MEMBER_MIN_DATE = "hasMemberMinYear";
        public static final String HAS_MEMBER_MAX_DATE = "hasMemberMaxYear";
        public static final String DATE_FORMAT = "dateFormat";
        public static final String TIMESTAMP_RESOLUTION = "relationshipTimestampResolution";
        public static final String NEO4J_SCHEMA = "neo4jSchema";
    }

    /*
     * Relationships
     */

    public static class Knows
    {
        public static final String CREATION_DATE = "creationDate";
        public static final String WEIGHT = "weight";
    }

    public static class StudiesAt
    {
        public static final String CLASS_YEAR = "classYear";
    }

    public static class WorksAt
    {
        public static final String WORK_FROM = "workFrom";
    }

    public static class Likes
    {
        public static final String CREATION_DATE = "creationDate";
    }

    public static class HasMember
    {
        public static final String JOIN_DATE = "joinDate";
    }
}
