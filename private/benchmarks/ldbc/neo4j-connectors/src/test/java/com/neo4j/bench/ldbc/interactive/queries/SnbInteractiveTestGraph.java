/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.queries;

import com.neo4j.bench.ldbc.QueryGraphMaker;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.helpers.collection.MapUtil;

import static com.neo4j.bench.ldbc.Domain.Forum;
import static com.neo4j.bench.ldbc.Domain.HasMember;
import static com.neo4j.bench.ldbc.Domain.Knows;
import static com.neo4j.bench.ldbc.Domain.Likes;
import static com.neo4j.bench.ldbc.Domain.Message;
import static com.neo4j.bench.ldbc.Domain.Nodes;
import static com.neo4j.bench.ldbc.Domain.Organisation;
import static com.neo4j.bench.ldbc.Domain.Person;
import static com.neo4j.bench.ldbc.Domain.Place;
import static com.neo4j.bench.ldbc.Domain.Post;
import static com.neo4j.bench.ldbc.Domain.Rels;
import static com.neo4j.bench.ldbc.Domain.StudiesAt;
import static com.neo4j.bench.ldbc.Domain.Tag;
import static com.neo4j.bench.ldbc.Domain.TagClass;
import static com.neo4j.bench.ldbc.Domain.WorksAt;

public class SnbInteractiveTestGraph
{
    public static class Query1GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   /*
                   * NOTES
                   */
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (f1:" + Nodes.Person + " $f1),\n"
                   + " (f2:" + Nodes.Person + " $f2),\n"
                   + " (f3:" + Nodes.Person + " $f3),\n"
                   + " (ff11:" + Nodes.Person + " $ff11),\n"
                   + " (fff111:" + Nodes.Person + " $fff111),\n"
                   + " (ffff1111:" + Nodes.Person + " $ffff1111),\n"
                   + " (fffff11111:" + Nodes.Person + " $fffff11111),\n"
                   + " (ff21:" + Nodes.Person + " $ff21),\n"
                   + " (fff211:" + Nodes.Person + " $fff211),\n"
                   + " (ff31:" + Nodes.Person + " $ff31),\n"
                   /*
                   * Universities
                   */
                   + " (uni0:" + Organisation.Type.University + " $uni0),\n"
                   + " (uni1:" + Organisation.Type.University + " $uni1),\n"
                   + " (uni2:" + Organisation.Type.University + " $uni2),\n"
                   /*
                   * Companies
                   */
                   + " (company0:" + Organisation.Type.Company + " $company0),\n"
                   + " (company1:" + Organisation.Type.Company + " $company1),\n"
                   /*
                   * Cities
                   */
                   + " (city0:" + Place.Type.City + " $city0),\n"
                   + " (city1:" + Place.Type.City + " $city1),\n"
                   /*
                   * Countries
                   */
                   + " (country0:" + Place.Type.Country + " $country0),\n"
                   + " (country1:" + Place.Type.Country + " $country1),\n"
                   /*
                   * RELATIONSHIP
                   */
                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                   + " (person0)-[:" + Rels.KNOWS + "]->(f1),\n"
                   + " (person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                   + " (person0)-[:" + Rels.KNOWS + "]->(f3),\n"
                   + " (f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   + " (f2)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   + " (f2)-[:" + Rels.KNOWS + "]->(ff21),\n"
                   + " (f3)-[:" + Rels.KNOWS + "]->(ff31),\n"
                   + " (ff11)-[:" + Rels.KNOWS + "]->(fff111),\n"
                   + " (fff111)-[:" + Rels.KNOWS + "]->(ffff1111),\n"
                   + " (ffff1111)-[:" + Rels.KNOWS + "]->(fffff11111),\n"
                   + " (ff21)-[:" + Rels.KNOWS + "]->(fff211),\n"
                   + " (f3)-[:" + Rels.KNOWS + "]->(f2),\n"
                   /*
                   * Person-City
                   */
                   + " (person0)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f1)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff11)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (fff111)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ffff1111)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (fffff11111)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f2)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (ff21)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (fff211)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (f3)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (ff31)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   /*
                    * City-Country
                    */
                   + " (city0)-[:" + Rels.IS_PART_OF + "]->(country0),\n"
                   + " (city1)-[:" + Rels.IS_PART_OF + "]->(country1),\n"
                   /*
                    * Company-Country
                    */
                   + " (company0)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + " ]->(country0),\n"
                   + " (company1)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + " ]->(country1),\n"
                   /*
                    * University-City
                    */
                   + " (uni0)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + " ]->(city1),\n"
                   + " (uni1)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + " ]->(city0),\n"
                   + " (uni2)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + " ]->(city0),\n"
                   /*
                    * Person-University
                    */
                   + " (f1)-[:" + Rels.STUDY_AT + " $f1StudyAtUni0]->(uni0),\n"
                   + " (ff11)-[:" + Rels.STUDY_AT + " $ff11StudyAtUni1]->(uni1),\n"
                   + " (ff11)-[:" + Rels.STUDY_AT + " $ff11StudyAtUni2]->(uni2),\n"
                   + " (f2)-[:" + Rels.STUDY_AT + " $f2StudyAtUni2]->(uni2),\n"
                   /*
                    * Person-Company
                    */
                   + " (f1)-[:" + Rels.WORKS_AT + " $f1WorkAtCompany0]->(company0),\n"
                   + " (f3)-[:" + Rels.WORKS_AT + " $f3WorkAtCompany0]->(company0),\n"
                   + " (ff21)-[:" + Rels.WORKS_AT + " $ff21WorkAtCompany1]->(company1)\n";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "ff11", TestPersons.ff11(),
                    "fff111", TestPersons.fff111(),
                    "ffff1111", TestPersons.ffff1111(),
                    "fffff11111", TestPersons.fffff11111(),
                    "ff21", TestPersons.ff21(),
                    "fff211", TestPersons.fff211(),
                    "ff31", TestPersons.ff31(),
                    // Universities
                    "uni0", TestUniversities.uni0(),
                    "uni1", TestUniversities.uni1(),
                    "uni2", TestUniversities.uni2(),
                    // Companies
                    "company0", TestCompanies.company0(),
                    "company1", TestCompanies.company1(),
                    // Cities
                    "city0", TestCities.city0(),
                    "city1", TestCities.city1(),
                    // Countries
                    "country0", TestCountries.country0(),
                    "country1", TestCountries.country1(),
                    // WorkAt
                    "f1WorkAtCompany0", TestWorkAt.f1WorkAtCompany0(),
                    "f3WorkAtCompany0", TestWorkAt.f3WorkAtCompany0(),
                    "ff21WorkAtCompany1", TestWorkAt.ff21WorkAtCompany1(),
                    // StudyAt
                    "f1StudyAtUni0", TestStudyAt.f1StudyAtUni0(),
                    "ff11StudyAtUni1", TestStudyAt.ff11StudyAtUni1(),
                    "ff11StudyAtUni2", TestStudyAt.ff11StudyAtUni2(),
                    "f2StudyAtUni2", TestStudyAt.f2StudyAtUni2()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 0L );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person0email1", "person0email2"} );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.LANGUAGES, new String[]{"person0language0", "person0language1"} );
                params.put( Person.LOCATION_IP, "ip0" );
                return params;
            }

            protected static Map<String,Object> f1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend1email1", "friend1email2"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"friend1language0"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last0-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"friend2language0", "friend2language1"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last0-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend3email1", "friend3email2"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"friend3language0"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> ff11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 11L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last11-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 11L );
                params.put( Person.BIRTHDAY, 11L );
                params.put( Person.BROWSER_USED, "browser11" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{} );
                params.put( Person.GENDER, "gender11" );
                params.put( Person.LANGUAGES, new String[]{} );
                params.put( Person.LOCATION_IP, "ip11" );
                return params;
            }

            protected static Map<String,Object> fff111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 111L );
                params.put( Person.FIRST_NAME, "name1" );
                params.put( Person.LAST_NAME, "last111-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 111L );
                params.put( Person.BIRTHDAY, 111L );
                params.put( Person.BROWSER_USED, "browser111" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"fff111email1", "fff111email2", "fff111email3"} );
                params.put( Person.GENDER, "gender111" );
                params.put( Person.LANGUAGES, new String[]{"fff111language0", "fff111language1", "fff111language2"} );
                params.put( Person.LOCATION_IP, "ip111" );
                return params;
            }

            protected static Map<String,Object> ffff1111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1111L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last1111-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 1111L );
                params.put( Person.BIRTHDAY, 1111L );
                params.put( Person.BROWSER_USED, "browser1111" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ffff1111email1"} );
                params.put( Person.GENDER, "gender1111" );
                params.put( Person.LANGUAGES, new String[]{"ffff1111language0"} );
                params.put( Person.LOCATION_IP, "ip1111" );
                return params;
            }

            protected static Map<String,Object> fffff11111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 11111L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last11111-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 11111L );
                params.put( Person.BIRTHDAY, 11111L );
                params.put( Person.BROWSER_USED, "browser11111" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"fffff11111email1"} );
                params.put( Person.GENDER, "gender11111" );
                params.put( Person.LANGUAGES, new String[]{"fffff11111language0"} );
                params.put( Person.LOCATION_IP, "ip11111" );
                return params;
            }

            protected static Map<String,Object> ff21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 21L );
                params.put( Person.FIRST_NAME, "name1" );
                params.put( Person.LAST_NAME, "last21-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 21L );
                params.put( Person.BIRTHDAY, 21L );
                params.put( Person.BROWSER_USED, "browser21" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{} );
                params.put( Person.GENDER, "gender21" );
                params.put( Person.LANGUAGES, new String[]{} );
                params.put( Person.LOCATION_IP, "ip21" );
                return params;
            }

            protected static Map<String,Object> fff211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 211L );
                params.put( Person.FIRST_NAME, "name1" );
                params.put( Person.LAST_NAME, "last211-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 211L );
                params.put( Person.BIRTHDAY, 211L );
                params.put( Person.BROWSER_USED, "browser211" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"fff211email1"} );
                params.put( Person.GENDER, "gender211" );
                params.put( Person.LANGUAGES, new String[]{} );
                params.put( Person.LOCATION_IP, "ip211" );
                return params;
            }

            protected static Map<String,Object> ff31()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 31L );
                params.put( Person.FIRST_NAME, "name0" );
                params.put( Person.LAST_NAME, "last31-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 31L );
                params.put( Person.BIRTHDAY, 31L );
                params.put( Person.BROWSER_USED, "browser31" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{} );
                params.put( Person.GENDER, "gender31" );
                params.put( Person.LANGUAGES, new String[]{} );
                params.put( Person.LOCATION_IP, "ip31" );
                return params;
            }
        }

        protected static class TestUniversities
        {
            protected static Map<String,Object> uni0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.ID, 0L );
                params.put( Organisation.NAME, "uni0" );
                return params;
            }

            protected static Map<String,Object> uni1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.ID, 1L );
                params.put( Organisation.NAME, "uni1" );
                return params;
            }

            protected static Map<String,Object> uni2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.ID, 2L );
                params.put( Organisation.NAME, "uni2" );
                return params;
            }
        }

        protected static class TestCompanies
        {
            protected static Map<String,Object> company0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.ID, 10L );
                params.put( Organisation.NAME, "company0" );
                return params;
            }

            protected static Map<String,Object> company1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.ID, 11L );
                params.put( Organisation.NAME, "company1" );
                return params;
            }
        }

        protected static class TestCities
        {
            protected static Map<String,Object> city0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 0L );
                params.put( Place.NAME, "city0" );
                return params;
            }

            protected static Map<String,Object> city1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 1L );
                params.put( Place.NAME, "city1" );
                return params;
            }
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 10L );
                params.put( Place.NAME, "country0" );
                return params;
            }

            protected static Map<String,Object> country1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 11L );
                params.put( Place.NAME, "country1" );
                return params;
            }
        }

        protected static class TestWorkAt
        {
            protected static Map<String,Object> f1WorkAtCompany0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 0 );
                return params;
            }

            protected static Map<String,Object> f3WorkAtCompany0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 1 );
                return params;
            }

            protected static Map<String,Object> ff21WorkAtCompany1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 2 );
                return params;
            }
        }

        protected static class TestStudyAt
        {
            protected static Map<String,Object> f1StudyAtUni0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( StudiesAt.CLASS_YEAR, 0 );
                return params;
            }

            protected static Map<String,Object> ff11StudyAtUni1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( StudiesAt.CLASS_YEAR, 1 );
                return params;
            }

            protected static Map<String,Object> ff11StudyAtUni2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( StudiesAt.CLASS_YEAR, 2 );
                return params;
            }

            protected static Map<String,Object> f2StudyAtUni2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( StudiesAt.CLASS_YEAR, 3 );
                return params;
            }
        }
    }

    public static class Query2GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Countries
                    */
                   + " (country0:" + Place.Type.Country + " $country0),\n"
                    /*
                     * Forums
                    */
                   + " (forum1:" + Nodes.Forum + " $forum1),"
                   /*
                    * Persons
                    */
                   + " (p1:" + Nodes.Person + " $p1), "
                   + "(f2:" + Nodes.Person + " $f2), "
                   + "(f3:" + Nodes.Person + " $f3), "
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + " (s5:" + Nodes.Person + " $s5), "
                   + "(ff6:" + Nodes.Person + " $ff6),"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   + " (s8:" + Nodes.Person + "),\n"
                   /*
                   * Posts
                   */
                   + " (f3Post1:" + Nodes.Post + ":" + Nodes.Message + " $f3Post1),"
                   + " (f3Post2:" + Nodes.Post + ":" + Nodes.Message + " $f3Post2),"
                   + " (f3Post3:" + Nodes.Post + ":" + Nodes.Message + " $f3Post3),\n"
                   + " (f4Post1:" + Nodes.Post + ":" + Nodes.Message + " $f4Post1),"
                   + " (f2Post1:" + Nodes.Post + ":" + Nodes.Message + " $f2Post1),"
                   + " (f2Post2:" + Nodes.Post + ":" + Nodes.Message + " $f2Post2),"
                   + " (f2Post3:" + Nodes.Post + ":" + Nodes.Message + " $f2Post3),\n"
                   + " (s5Post1:" + Nodes.Post + ":" + Nodes.Message + " $s5Post1),"
                   + " (s5Post2:" + Nodes.Post + ":" + Nodes.Message + " $s5Post2),"
                   + " (ff6Post1:" + Nodes.Post + ":" + Nodes.Message + " $ff6Post1),\n"
                   + " (s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),"
                   + " (s7Post2:" + Nodes.Post + ":" + Nodes.Message + " $s7Post2),\n"
                   /*
                   * Comments
                   */
                   + " (f2Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f2Comment1),"
                   + " (f2Comment2:" + Nodes.Comment + ":" + Nodes.Message + " $f2Comment2),\n"
                   + " (s5Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $s5Comment1),"
                   + " (f3Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f3Comment1),"
                   + " (p1Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $p1Comment1)\n"
                   /*
                   * RELATIONSHIP
                   */
                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                   + "FOREACH (n IN [f3, f2, f4] | CREATE (p1)-[:" + Rels.KNOWS + "]->(n) )\n"
                   + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f4) )\n"
                   + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s7) )\n"
                   + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Post-Forum
                    */
                   +
                   "FOREACH (n IN [f3Post1, f3Post2, f3Post3, f4Post1, f2Post1, f2Post2, f2Post3, s5Post1, s5Post2, " +
                   "s7Post1, s7Post2, ff6Post1]| CREATE (forum1)-[:" +
                   Rels.CONTAINER_OF + "]->(n) )\n"
                   /*
                    * Person-MODERATOR-Forum
                    */
                   + "FOREACH (n IN [s8]| CREATE (forum1)-[:" + Rels.HAS_MODERATOR + "]->(n) )\n"
                   /*
                    * Person-HAS_MEMBER-Forum
                    */
                   + "FOREACH (n IN [p1,f2,f3,f4,s5,ff6,s7,s8]| CREATE (forum1)-[:" + Rels.HAS_MEMBER + "{" +
                   HasMember.JOIN_DATE + ":1}]->(n) )\n"
                   /*
                    * Comment-Person
                    */
                   + "FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [p1Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(p1) )\n"
                   + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [s5Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(s5) )\n"
                   /*
                    * Comment-Post
                    */
                   + "FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f3Post2) )\n"
                   + "FOREACH (n IN [f2Comment2] | CREATE (n)-[:" + Rels.REPLY_OF_COMMENT + "]->(s5Comment1) )\n"
                   + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f4Post1) )\n"
                   + "FOREACH (n IN [p1Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f2Post2) )";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "country0", TestCountries.country0(),
                    "forum1", TestForums.forum1(),
                    "p1", TestPersons.p1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1(),
                    "f2Comment1", TestComments.f2Comment1(),
                    "f2Comment2", TestComments.f2Comment2(),
                    "s5Comment1", TestComments.s5Comment1(),
                    "f3Comment1", TestComments.f3Comment1(),
                    "p1Comment1", TestComments.p1Comment1()
            );
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 10L );
                params.put( Place.NAME, "country0" );
                return params;
            }
        }

        protected static class TestForums
        {
            protected static Map<String,Object> forum1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 1L );
                params.put( Forum.TITLE, "forum1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Forum.CREATION_DATE, 0L );
                return params;
            }
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> p1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1@email1", "person1@email2"} );
                params.put( Person.GENDER, "1" );
                params.put( Person.LANGUAGES, new String[]{"1a", "1b"} );
                params.put( Person.LOCATION_IP, "1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend2@email1"} );
                params.put( Person.GENDER, "2" );
                params.put( Person.LANGUAGES, new String[]{"2"} );
                params.put( Person.LOCATION_IP, "2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend3@email1", "friend3@email2"} );
                params.put( Person.GENDER, "3" );
                params.put( Person.LANGUAGES, new String[]{"3a", "3b"} );
                params.put( Person.LOCATION_IP, "3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 4L );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.BROWSER_USED, "4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend4@email1"} );
                params.put( Person.GENDER, "4" );
                params.put( Person.LANGUAGES, new String[]{"4a", "4b"} );
                params.put( Person.LOCATION_IP, "4" );
                return params;
            }

            protected static Map<String,Object> s5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "s5" );
                params.put( Person.LAST_NAME, "last5-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"stranger5@email1"} );
                params.put( Person.GENDER, "5" );
                params.put( Person.LANGUAGES, new String[]{"5"} );
                params.put( Person.LOCATION_IP, "5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 6L );
                params.put( Person.BIRTHDAY, 6L );
                params.put( Person.BROWSER_USED, "6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff6@email1"} );
                params.put( Person.GENDER, "6" );
                params.put( Person.LANGUAGES, new String[]{"6a", "6b"} );
                params.put( Person.LOCATION_IP, "6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s7@email1"} );
                params.put( Person.GENDER, "7" );
                params.put( Person.LANGUAGES, new String[]{"7"} );
                params.put( Person.LOCATION_IP, "7" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> f3Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "[f3Post1] content" );
                params.put( Post.LANGUAGE, "3" );
                params.put( Post.IMAGE_FILE, "3" );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.BROWSER_USED, "3" );
                params.put( Message.LOCATION_IP, "3" );
                return params;
            }

            protected static Map<String,Object> f3Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "[f3Post2] content" );
                params.put( Post.LANGUAGE, "3" );
                params.put( Post.IMAGE_FILE, "[f3Post2] image" );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.BROWSER_USED, "3" );
                params.put( Message.LOCATION_IP, "3" );
                return params;
            }

            protected static Map<String,Object> f3Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Post.LANGUAGE, "3" );
                params.put( Post.IMAGE_FILE, "[f3Post3] image" );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.BROWSER_USED, "3" );
                params.put( Message.LOCATION_IP, "3" );
                return params;
            }

            protected static Map<String,Object> f4Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CONTENT, "[f4Post1] content" );
                params.put( Post.LANGUAGE, "4" );
                params.put( Post.IMAGE_FILE, "[f4Post1] image" );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.BROWSER_USED, "4" );
                params.put( Message.LOCATION_IP, "4" );
                return params;
            }

            protected static Map<String,Object> f2Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "[f2Post1] content" );
                params.put( Post.LANGUAGE, "2" );
                params.put( Post.IMAGE_FILE, "[f2Post1] image" );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.BROWSER_USED, "2" );
                params.put( Message.LOCATION_IP, "2" );
                return params;
            }

            protected static Map<String,Object> f2Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "[f2Post2] content" );
                params.put( Post.LANGUAGE, "2" );
                params.put( Post.IMAGE_FILE, "[f2Post2] image" );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.BROWSER_USED, "2" );
                params.put( Message.LOCATION_IP, "2" );
                return params;
            }

            protected static Map<String,Object> f2Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 7L );
                params.put( Message.CONTENT, "[f2Post3] content" );
                params.put( Post.LANGUAGE, "2" );
                params.put( Post.IMAGE_FILE, "[f2Post3] image" );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.BROWSER_USED, "safari" );
                params.put( Message.LOCATION_IP, "31.55.91.343" );
                return params;
            }

            protected static Map<String,Object> s5Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 8L );
                params.put( Message.CONTENT, "[s5Post1] content" );
                params.put( Post.LANGUAGE, "5" );
                params.put( Post.IMAGE_FILE, "[s5Post1] image" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "5" );
                params.put( Message.LOCATION_IP, "5" );
                return params;
            }

            protected static Map<String,Object> s5Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 9L );
                params.put( Message.CONTENT, "[s5Post2] content" );
                params.put( Post.LANGUAGE, "5" );
                params.put( Post.IMAGE_FILE, "[s5Post2] image" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "5" );
                params.put( Message.LOCATION_IP, "5" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CONTENT, "[s7Post1] content" );
                params.put( Post.LANGUAGE, "7a" );
                params.put( Post.IMAGE_FILE, "[s7Post1] image" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "7" );
                params.put( Message.LOCATION_IP, "7" );
                return params;
            }

            protected static Map<String,Object> s7Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "[s7Post2] content" );
                params.put( Post.LANGUAGE, "7" );
                params.put( Post.IMAGE_FILE, "[s7Post2] image" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "7" );
                params.put( Message.LOCATION_IP, "7" );
                return params;
            }

            protected static Map<String,Object> ff6Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "[ff6Post1] content" );
                params.put( Post.LANGUAGE, "6" );
                params.put( Post.IMAGE_FILE, "[ff6Post1] image" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "6" );
                params.put( Message.LOCATION_IP, "6" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> f2Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 13L );
                params.put( Message.CONTENT, "[f2Comment1] content" );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.BROWSER_USED, "2" );
                params.put( Message.LOCATION_IP, "2" );
                return params;
            }

            protected static Map<String,Object> f2Comment2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 14L );
                params.put( Message.CONTENT, "[f2Comment2] content" );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.BROWSER_USED, "2" );
                params.put( Message.LOCATION_IP, "2" );
                return params;
            }

            protected static Map<String,Object> s5Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 15L );
                params.put( Message.CONTENT, "[s5Comment1] content" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "5" );
                params.put( Message.LOCATION_IP, "5" );
                return params;
            }

            protected static Map<String,Object> f3Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 16L );
                params.put( Message.CONTENT, "[f3Comment1] content" );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.BROWSER_USED, "3" );
                params.put( Message.LOCATION_IP, "3" );
                return params;
            }

            protected static Map<String,Object> p1Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 17L );
                params.put( Message.CONTENT, "[p1Comment1] content" );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.BROWSER_USED, "browser1" );
                params.put( Message.LOCATION_IP, "1" );
                return params;
            }
        }
    }

    public static class Query3GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person1:" + Nodes.Person + " $person1), "
                   + "(f2:" + Nodes.Person + " $f2), "
                   + "(f3:" + Nodes.Person + " $f3), "
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + " (s5:" + Nodes.Person + " $s5), "
                   + "(ff6:" + Nodes.Person + " $ff6),"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   /*
                   * Cities
                   */
                   + " (city1:" + Place.Type.City + " $city1), "
                   + " (city2:" + Place.Type.City + " $city2),"
                   + " (city3:" + Place.Type.City + " $city3),\n"
                   + " (city4:" + Place.Type.City + " $city4),"
                   + " (city5:" + Place.Type.City + " $city5),\n"
                   /*
                   * Countries
                   */
                   + " (country1:" + Place.Type.Country + " $country1),"
                   + " (country2:" + Place.Type.Country + " $country2),\n"
                   + " (country3:" + Place.Type.Country + " $country3),"
                   + " (country4:" + Place.Type.Country + " $country4),\n"
                   + " (country5:" + Place.Type.Country + " $country5),\n"
                   /*
                   * Posts
                   */
                   + " (f3Post1:" + Nodes.Post + ":" + Nodes.Message + " $f3Post1),\n"
                   + "(f3Post2:" + Nodes.Post + ":" + Nodes.Message + " $f3Post2),\n"
                   + " (f3Post3:" + Nodes.Post + ":" + Nodes.Message + " $f3Post3),\n"
                   + " (f4Post1:" + Nodes.Post + ":" + Nodes.Message + " $f4Post1),\n"
                   + "(f2Post1:" + Nodes.Post + ":" + Nodes.Message + " $f2Post1),\n"
                   + " (f2Post2:" + Nodes.Post + ":" + Nodes.Message + " $f2Post2),\n"
                   + "(f2Post3:" + Nodes.Post + ":" + Nodes.Message + " $f2Post3),\n"
                   + " (s5Post1:" + Nodes.Post + ":" + Nodes.Message + " $s5Post1),\n"
                   + " (s5Post2:" + Nodes.Post + ":" + Nodes.Message + " $s5Post2),\n"
                   + " (ff6Post1:" + Nodes.Post + ":" + Nodes.Message + " $ff6Post1),\n"
                   + " (s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),\n"
                   + " (s7Post2:" + Nodes.Post + ":" + Nodes.Message + " $s7Post2),\n"
                   /*
                   * Comments
                   */
                   + " (f2Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f2Comment1),"
                   + " (f2Comment2:" + Nodes.Comment + ":" + Nodes.Message + " $f2Comment2),\n"
                   + " (s5Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $s5Comment1),"
                   + " (f3Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f3Comment1),"
                   + " (person1Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $person1Comment1),\n"
                   + " (ff6Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $ff6Comment1),\n"
                   /*
                   * RELATIONSHIP
                   */
                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * City-Country
                    */
                   + " (city2)-[:" + Rels.IS_PART_OF + "]->(country1),"
                   + " (city1)-[:" + Rels.IS_PART_OF + "]->(country2),\n"
                   + " (city3)-[:" + Rels.IS_PART_OF + "]->(country3),"
                   + " (city4)-[:" + Rels.IS_PART_OF + "]->(country5),"
                   + " (city5)-[:" + Rels.IS_PART_OF + "]->(country4),\n"
                   /*
                    * Person-City
                    */
                   + " (ff6)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city4),\n"
                   + " (person1)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city2),\n"
                   + " (f2)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city4),\n"
                   + " (f3)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (f4)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city3),\n"
                   + " (s5)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city2),\n"
                   + " (s7)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city2),\n"
                   /*
                   * Comment-Country
                   */
                   + " (s5Comment1)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country1),\n"
                   + " (f2Comment2)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country4),\n"
                   + " (f3Comment1)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country3),\n"
                   + " (person1Comment1)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country2),\n"
                   + " (f2Comment1)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country1),\n"
                   + " (ff6Comment1)-[:" + Rels.COMMENT_IS_LOCATED_IN + "]->(country2)\n"
                   /*
                   * Post-Country
                   */
                   + "FOREACH (n IN [f3Post1,f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_IS_LOCATED_IN +
                   "]->(country2) )\n"
                   + "FOREACH (n IN [ff6Post1,f3Post2,f3Post3,f2Post1,s7Post1,s7Post2,s5Post2] | CREATE (n)-[:" +
                   Rels.POST_IS_LOCATED_IN + "]->(country1) )\n"
                   + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.POST_IS_LOCATED_IN + "]->(country3) )\n"
                   + "FOREACH (n IN [s5Post1] | CREATE (n)-[:" + Rels.POST_IS_LOCATED_IN + "]->(country4) )\n"
                   /*
                   * Person-Person
                   */
                   + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                   + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f4) )\n"
                   + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s7) )\n"
                   + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Comment-Person
                    */
                   + "FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [person1Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(person1) )\n"
                   + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [s5Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [ff6Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Comment-Post
                    */
                   + "FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f3Post2) )\n"
                   + "FOREACH (n IN [f2Comment2] | CREATE (n)-[:" + Rels.REPLY_OF_COMMENT + "]->(s5Comment1) )\n"
                   + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f4Post1) )\n"
                   + "FOREACH (n IN [person1Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f2Post2) )\n"
                   + "FOREACH (n IN [ff6Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f2Post3) )\n";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "city1", TestCities.city1(),
                    "city2", TestCities.city2(),
                    "city3", TestCities.city3(),
                    "city4", TestCities.city4(),
                    "city5", TestCities.city5(),
                    "country1", TestCountries.country1(),
                    "country2", TestCountries.country2(),
                    "country3", TestCountries.country3(),
                    "country4", TestCountries.country4(),
                    "country5", TestCountries.country5(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1(),
                    "f2Comment1", TestComments.f2Comment1(),
                    "f2Comment2", TestComments.f2Comment2(),
                    "s5Comment1", TestComments.s5Comment1(),
                    "f3Comment1", TestComments.f3Comment1(),
                    "ff6Comment1", TestComments.ff6Comment1(),
                    "person1Comment1", TestComments.person1Comment1() );
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 11L );
                params.put( Place.NAME, "country1" );
                return params;
            }

            protected static Map<String,Object> country2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 12L );
                params.put( Place.NAME, "country2" );
                return params;
            }

            protected static Map<String,Object> country3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 13L );
                params.put( Place.NAME, "country3" );
                return params;
            }

            protected static Map<String,Object> country4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 14L );
                params.put( Place.NAME, "country4" );
                return params;
            }

            protected static Map<String,Object> country5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 15L );
                params.put( Place.NAME, "country5" );
                return params;
            }
        }

        protected static class TestCities
        {
            protected static Map<String,Object> city1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 1L );
                params.put( Place.NAME, "city1" );
                return params;
            }

            protected static Map<String,Object> city2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 2L );
                params.put( Place.NAME, "city2" );
                return params;
            }

            protected static Map<String,Object> city3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 3L );
                params.put( Place.NAME, "city3" );
                return params;
            }

            protected static Map<String,Object> city4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 4L );
                params.put( Place.NAME, "city4" );
                return params;
            }

            protected static Map<String,Object> city5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 5L );
                params.put( Place.NAME, "city5" );
                return params;
            }
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1a@email.com", "person1b@email.com"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"language1a", "language1b"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"language2"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f3a@email.com", "f3b@email.com"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"language3a", "language3b"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 4L );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"} );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.LANGUAGES, new String[]{"language4a", "language4b"} );
                params.put( Person.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> s5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "s5" );
                params.put( Person.LAST_NAME, "last5-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "browser5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s5@email.com"} );
                params.put( Person.GENDER, "gender5" );
                params.put( Person.LANGUAGES, new String[]{"language5"} );
                params.put( Person.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 6L );
                params.put( Person.BIRTHDAY, 6L );
                params.put( Person.BROWSER_USED, "browser6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"} );
                params.put( Person.GENDER, "gender6" );
                params.put( Person.LANGUAGES, new String[]{"language6a", "language6b"} );
                params.put( Person.LOCATION_IP, "ip6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "browser7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"} );
                params.put( Person.GENDER, "gender7" );
                params.put( Person.LANGUAGES, new String[]{"language7"} );
                params.put( Person.LOCATION_IP, "ip7" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> f3Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "[f3Post1] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "[f3Post2] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 10 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "[f3Post3] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 4 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CONTENT, "[f4Post1] content" );
                params.put( Post.LANGUAGE, "language4" );
                params.put( Post.IMAGE_FILE, "image4" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser4" );
                params.put( Message.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> f2Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "[f2Post1] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "ip2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 10 ) );
                params.put( Message.BROWSER_USED, "ip2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "[f2Post2] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 10 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 7L );
                params.put( Message.CONTENT, "[f2Post3] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 4 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> s5Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 8L );
                params.put( Message.CONTENT, "[s5Post1] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s5Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 9L );
                params.put( Message.CONTENT, "[s5Post2] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CONTENT, "[s7Post1] content" );
                params.put( Post.LANGUAGE, "language7a" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> s7Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "[s7Post2] content" );
                params.put( Post.LANGUAGE, "language7" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> ff6Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "[ff6Post1] content" );
                params.put( Post.LANGUAGE, "language6" );
                params.put( Post.IMAGE_FILE, "image6" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 4 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> f2Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 13L );
                params.put( Message.CONTENT, "[f2Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Comment2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 14L );
                params.put( Message.CONTENT, "[f2Comment2] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 4 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> s5Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 15L );
                params.put( Message.CONTENT, "[s5Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> f3Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 16L );
                params.put( Message.CONTENT, "[f3Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> person1Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 17L );
                params.put( Message.CONTENT, "[person1Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser1" );
                params.put( Message.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> ff6Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 18L );
                params.put( Message.CONTENT, "[ff6Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 4 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }
    }

    public static class Query4GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Tags
                    */
                   + " (tag1:" + Nodes.Tag + " $tag1), "
                   + "(tag2:" + Nodes.Tag + " $tag2), "
                   + "(tag3:" + Nodes.Tag + " $tag3), "
                   + "(tag4:" + Nodes.Tag + " $tag4), "
                   + "(tag5:" + Nodes.Tag + " $tag5),\n"
                   /*
                    * Persons
                    */
                   + " (person1:" + Nodes.Person + " $person1), "
                   + "(f2:" + Nodes.Person + " $f2), "
                   + "(f3:" + Nodes.Person + " $f3), "
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + "(s5:" + Nodes.Person + " $s5), "
                   + "(ff6:" + Nodes.Person + " $ff6),"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   /*
                   * Posts
                   */
                   + " (f3Post1:" + Nodes.Post + ":" + Nodes.Message + " $f3Post1),"
                   + " (f3Post2:" + Nodes.Post + ":" + Nodes.Message + " $f3Post2),"
                   + " (f3Post3:" + Nodes.Post + ":" + Nodes.Message + " $f3Post3),\n"
                   + " (f4Post1:" + Nodes.Post + ":" + Nodes.Message + " $f4Post1),"
                   + " (f2Post1:" + Nodes.Post + ":" + Nodes.Message + " $f2Post1),"
                   + " (f2Post2:" + Nodes.Post + ":" + Nodes.Message + " $f2Post2),"
                   + " (f2Post3:" + Nodes.Post + ":" + Nodes.Message + " $f2Post3),\n"
                   + " (s5Post1:" + Nodes.Post + ":" + Nodes.Message + " $s5Post1),"
                   + " (s5Post2:" + Nodes.Post + ":" + Nodes.Message + " $s5Post2),"
                   + " (ff6Post1:" + Nodes.Post + ":" + Nodes.Message + " $ff6Post1),\n"
                   + " (s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),"
                   + " (s7Post2:" + Nodes.Post + ":" + Nodes.Message + " $s7Post2),\n"
                   /*
                   * Comments
                   */
                   + " (f4Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f4Comment1)\n"
                   /*
                   * RELATIONSHIP
                   */
                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                   + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                   + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f4) )\n"
                   + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s7) )\n"
                   + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                   * Comment-Person
                   */
                   + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_CREATOR + "]->(f4) )\n"
                   /*
                   * Comment-Post
                   */
                   + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.REPLY_OF_POST + "]->(f2Post1) )\n"
                   /*
                   * Post-Tag
                   */
                   + "FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG + "]->(tag4) )\n"
                   + "FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_TAG + "]->(tag5) )\n"
                   + "FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:" +
                   Rels.POST_HAS_TAG + "]->(tag3) )\n"
                   + "FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG +
                   "]->(tag2) )\n"
                   + "FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG +
                   "]->(tag1) )"
                  /*
                   * Post-Tag
                   */
                   + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.COMMENT_HAS_TAG + "]->(tag1) )";

        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "tag1", TestTags.tag1(),
                    "tag2", TestTags.tag2(),
                    "tag3", TestTags.tag3(),
                    "tag4", TestTags.tag4(),
                    "tag5", TestTags.tag5(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "f4Comment1", TestComments.f4Comment1(),
                    "ff6Post1", TestPosts.ff6Post1() );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1b@email.com", "person1b@email.com"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"language1a", "language1b"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend2@email.com"} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"language2"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend3a@email.com", "friend3b@email.com"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"language3a", "language3b"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend4@email.com"} );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.LANGUAGES, new String[]{"language4a", "language4b"} );
                params.put( Person.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> s5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "s5" );
                params.put( Person.LAST_NAME, "last5" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "browser5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"stranger5@email.com"} );
                params.put( Person.GENDER, "gender5" );
                params.put( Person.LANGUAGES, new String[]{"language5"} );
                params.put( Person.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"friend6@email.com"} );
                params.put( Person.GENDER, "gender6" );
                params.put( Person.LANGUAGES, new String[]{"language6a", "language6b"} );
                params.put( Person.LOCATION_IP, "ip6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "browser7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"stranger7a@email.com", "stranger7b@email.com"} );
                params.put( Person.GENDER, "gender7" );
                params.put( Person.LANGUAGES, new String[]{"language7a", "language7b"} );
                params.put( Person.LOCATION_IP, "ip7" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> f3Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "[f3Post1] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 2 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "[f3Post2] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "[f3Post3] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CONTENT, "[f4Post1] content" );
                params.put( Post.LANGUAGE, "language4" );
                params.put( Post.IMAGE_FILE, "image4" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 5 ) );
                params.put( Message.BROWSER_USED, "browser4" );
                params.put( Message.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> f2Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "[f2Post1] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "[f2Post2] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 7L );
                params.put( Message.CONTENT, "[f2Post3] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> s5Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 8L );
                params.put( Message.CONTENT, "[s5Post1] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 2 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s5Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 9L );
                params.put( Message.CONTENT, "[s5Post2] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CONTENT, "[s7Post1] content" );
                params.put( Post.LANGUAGE, "language7a" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> s7Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "[s7Post2] content" );
                params.put( Post.LANGUAGE, "language7" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> ff6Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "[ff6Post1] content" );
                params.put( Post.LANGUAGE, "language6" );
                params.put( Post.IMAGE_FILE, "image6" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> f4Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 13L );
                params.put( Message.CONTENT, "[f4Comment1] content" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser4" );
                params.put( Message.LOCATION_IP, "ip4" );
                return params;
            }
        }

        protected static class TestTags
        {
            protected static Map<String,Object> tag1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag1-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag2-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag3-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag4-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag5-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }
    }

    public static class Query5GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                   + "\n// --- NODES ---\n\n"
                    /*
                     * Forums
                    */
                   + " (forum1:" + Nodes.Forum + " $forum1),"
                   + " (forum2:" + Nodes.Forum + " $forum2),\n"
                   + " (forum3:" + Nodes.Forum + " $forum3),"
                   + " (forum4:" + Nodes.Forum + " $forum4),\n"
                   /*
                    * Persons
                    */
                   + " (person1:" + Nodes.Person + " $person1), "
                   + "(f2:" + Nodes.Person + " $f2), "
                   + "(f3:" + Nodes.Person + " $f3), "
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + " (s5:" + Nodes.Person + " $s5), "
                   + "(ff6:" + Nodes.Person + " $ff6),"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   /*
                   * Countries
                   */
                   + " (country0:" + Place.Type.Country + " $country0),\n"
                   + " (country1:" + Place.Type.Country + " $country1),\n"
                   /*
                   * Posts
                   */
                   + " (f3Post1:" + Nodes.Post + ":" + Nodes.Message + " $f3Post1),"
                   + " (f3Post2:" + Nodes.Post + ":" + Nodes.Message + " $f3Post2),"
                   + " (f3Post3:" + Nodes.Post + ":" + Nodes.Message + " $f3Post3),\n"
                   + " (f2Post1:" + Nodes.Post + ":" + Nodes.Message + " $f2Post1),"
                   + " (f2Post2:" + Nodes.Post + ":" + Nodes.Message + " $f2Post2),"
                   + " (f2Post3:" + Nodes.Post + ":" + Nodes.Message + " $f2Post3),\n"
                   + " (s5Post1:" + Nodes.Post + ":" + Nodes.Message + " $s5Post1),"
                   + " (s5Post2:" + Nodes.Post + ":" + Nodes.Message + " $s5Post2),"
                   + " (ff6Post1:" + Nodes.Post + ":" + Nodes.Message + " $ff6Post1),\n"
                   + " (s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),"
                   + " (s7Post2:" + Nodes.Post + ":" + Nodes.Message + " $s7Post2),\n"
                   /*
                   * RELATIONSHIP
                   */
                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Forum-Person (member)
                    */
                   + " (forum1)-[:" + Rels.HAS_MEMBER + " $forum1HasMemberPerson1]->(person1),"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + " $forum1HasMemberF2]->(f2),\n"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + " $forum1HasMemberS5]->(s5),"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + " $forum1HasMemberF3]->(f3),\n"
                   + " (forum1)-[:" + Rels.HAS_MEMBER + " $forum1HasMemberFF6]->(ff6),\n"
                   + " (forum2)-[:" + Rels.HAS_MEMBER + " $forum2HasMemberF3]->(f3),"
                   + " (forum3)-[:" + Rels.HAS_MEMBER + " $forum3HasMemberPerson1]->(person1),\n"
                   + " (forum3)-[:" + Rels.HAS_MEMBER + " $forum3HasMemberF3]->(f3),"
                   + " (forum3)-[:" + Rels.HAS_MEMBER + " $forum3HasMemberF4]->(f4),\n"
                   + " (forum4)-[:" + Rels.HAS_MEMBER + " $forum4HasMemberF2]->(f2),\n"
                   + " (forum4)-[:" + Rels.HAS_MEMBER + " $forum4HasMemberPerson1]->(person1),\n"
                   /*
                   * Person-Person
                   */
                   + " (person1)-[:" + Rels.KNOWS + "]->(f2), (f2)-[:" + Rels.KNOWS + "]->(ff6),\n"
                   + " (person1)-[:" + Rels.KNOWS + "]->(f3),\n"
                   + " (person1)-[:" + Rels.KNOWS + "]->(f4)-[:" + Rels.KNOWS + "]->(f2),\n"
                   + " (f4)-[:" + Rels.KNOWS + "]->(ff6)\n"
                   /*
                   * Post-Person
                   */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f4) )\n"
                   + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s7) )\n"
                   + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Post-Country
                    */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3, s5Post1, s5Post2, ff6Post1] | CREATE (n)-[:" +
                   Rels.POST_IS_LOCATED_IN + "]->(country0) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3, s7Post1, s7Post2] | CREATE (n)-[:" +
                   Rels.POST_IS_LOCATED_IN + "]->(country1) )\n"
                   /*
                    * Post-Forum
                    */
                   +
                   "FOREACH (n IN [f3Post1, f3Post2, f2Post1, f2Post2, s5Post1, s5Post2, ff6Post1, s7Post1, s7Post2]|" +
                   " CREATE (forum1)-[:" +
                   Rels.CONTAINER_OF + "]->(n) )\n"
                   + "FOREACH (n IN [f3Post3] | CREATE (forum3)-[:" + Rels.CONTAINER_OF + "]->(n) )\n"
                   + "FOREACH (n IN [f2Post3] | CREATE (forum4)-[:" + Rels.CONTAINER_OF + "]->(n) )";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "forum1", TestForums.forum1(),
                    "forum2", TestForums.forum2(),
                    "forum3", TestForums.forum3(),
                    "forum4", TestForums.forum4(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "country0", TestCountries.country0(),
                    "country1", TestCountries.country1(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1(),
                    "forum1HasMemberPerson1", TestHasMember.forum1HasMemberPerson1(),
                    "forum1HasMemberF2", TestHasMember.forum1HasMemberF2(),
                    "forum1HasMemberS5", TestHasMember.forum1HasMemberS5(),
                    "forum1HasMemberF3", TestHasMember.forums1HasMemberF3(),
                    "forum1HasMemberFF6", TestHasMember.forum1HasMemberFF6(),
                    "forum2HasMemberF3", TestHasMember.forum2HasMemberF3(),
                    "forum3HasMemberF3", TestHasMember.forum3HasMemberF3(),
                    "forum3HasMemberPerson1", TestHasMember.forum3HasMemberPerson1(),
                    "forum3HasMemberF4", TestHasMember.forum3HasMemberF4(),
                    "forum4HasMemberF2", TestHasMember.forum4HasMemberF2(),
                    "forum4HasMemberPerson1", TestHasMember.forum4HasMemberPerson1() );
        }

        protected static class TestHasMember
        {
            protected static Map<String,Object> forum1HasMemberPerson1()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum1HasMemberF2()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 1 ) );
            }

            protected static Map<String,Object> forum1HasMemberS5()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forums1HasMemberF3()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 1 ) );
            }

            protected static Map<String,Object> forum1HasMemberFF6()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum2HasMemberF3()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum3HasMemberF3()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum3HasMemberPerson1()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum3HasMemberF4()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }

            protected static Map<String,Object> forum4HasMemberF2()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 1 ) );
            }

            protected static Map<String,Object> forum4HasMemberPerson1()
            {
                return MapUtil.map( HasMember.JOIN_DATE, date( 2000, 1, 3 ) );
            }
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 10L );
                params.put( Place.NAME, "country0" );
                return params;
            }

            protected static Map<String,Object> country1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 11L );
                params.put( Place.NAME, "country1" );
                return params;
            }
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1a@email.com", "person1b@email.com"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"language1a", "language1b"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"language2"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f3a@email.com", "f3b@email.com"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"language3a", "language3b"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4" );
                params.put( Person.CREATION_DATE, 4L );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"} );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.LANGUAGES, new String[]{"language4a", "language4b"} );
                params.put( Person.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> s5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "s5" );
                params.put( Person.LAST_NAME, "last5" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "browser5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s5@email.com"} );
                params.put( Person.GENDER, "gender5" );
                params.put( Person.LANGUAGES, new String[]{"language5"} );
                params.put( Person.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6" );
                params.put( Person.CREATION_DATE, 6L );
                params.put( Person.BIRTHDAY, 6L );
                params.put( Person.BROWSER_USED, "browser6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"} );
                params.put( Person.GENDER, "gender6" );
                params.put( Person.LANGUAGES, new String[]{"language6a", "language6b"} );
                params.put( Person.LOCATION_IP, "ip6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "browser7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"} );
                params.put( Person.GENDER, "gender7" );
                params.put( Person.LANGUAGES, new String[]{"language7a", "language7b"} );
                params.put( Person.LOCATION_IP, "ip7" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> f3Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "[f3Post1] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "[f3Post2] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "[f3Post3] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f2Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "[f2Post1] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "[f2Post2] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 7L );
                params.put( Message.CONTENT, "[f2Post3] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> s5Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 8L );
                params.put( Message.CONTENT, "[s5Post1] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s5Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 9L );
                params.put( Message.CONTENT, "[s5Post2] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CONTENT, "[s7Post1] content" );
                params.put( Post.LANGUAGE, "language7a" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> s7Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "[s7Post2] content" );
                params.put( Post.LANGUAGE, "language7" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> ff6Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "[ff6Post1] content" );
                params.put( Post.LANGUAGE, "language6" );
                params.put( Post.IMAGE_FILE, "image6" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }

        protected static class TestForums
        {
            protected static Map<String,Object> forum1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 1L );
                params.put( Forum.TITLE, "forum1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Forum.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> forum2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 2L );
                params.put( Forum.TITLE, "forum1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Forum.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> forum3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 3L );
                params.put( Forum.TITLE, "forum3-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Forum.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> forum4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 4L );
                params.put( Forum.TITLE, "forum4-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Forum.CREATION_DATE, 1L );
                return params;
            }
        }
    }

    public static class Query6GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"

                   + "\n// --- NODES ---\n\n"

                   /*
                    * Tags
                    */
                   + " (tag1:" + Nodes.Tag + " $tag1), "
                   + "(tag2:" + Nodes.Tag + " $tag2), "
                   + "(tag3:" + Nodes.Tag + " $tag3), "
                   + "(tag4:" + Nodes.Tag + " $tag4), "
                   + "(tag5:" + Nodes.Tag + " $tag5),\n"
                   /*
                    * Persons
                    */
                   + " (person1:" + Nodes.Person + " $person1), "
                   + "(f2:" + Nodes.Person + " $f2), "
                   + "(f3:" + Nodes.Person + " $f3), "
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + " (s5:" + Nodes.Person + " $s5), "
                   + "(ff6:" + Nodes.Person + " $ff6),"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   /*
                   * Posts
                   */
                   + " (f3Post1:" + Nodes.Post + ":" + Nodes.Message + " $f3Post1),"
                   + " (f3Post2:" + Nodes.Post + ":" + Nodes.Message + " $f3Post2),"
                   + " (f3Post3:" + Nodes.Post + ":" + Nodes.Message + " $f3Post3),\n"
                   + " (f4Post1:" + Nodes.Post + ":" + Nodes.Message + " $f4Post1),"
                   + " (f2Post1:" + Nodes.Post + ":" + Nodes.Message + " $f2Post1),"
                   + " (f2Post2:" + Nodes.Post + ":" + Nodes.Message + " $f2Post2),"
                   + " (f2Post3:" + Nodes.Post + ":" + Nodes.Message + " $f2Post3),\n"
                   + " (s5Post1:" + Nodes.Post + ":" + Nodes.Message + " $s5Post1),"
                   + " (s5Post2:" + Nodes.Post + ":" + Nodes.Message + " $s5Post2),"
                   + " (ff6Post1:" + Nodes.Post + ":" + Nodes.Message + " $ff6Post1),\n"
                   + " (s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),"
                   + " (s7Post2:" + Nodes.Post + ":" + Nodes.Message + " $s7Post2)\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"

                   /*
                   * Person-Person
                   */
                   + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                   + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                   + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f3) )\n"
                   + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f2) )\n"
                   + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(f4) )\n"
                   + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s5) )\n"
                   + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(s7) )\n"
                   + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_CREATOR + "]->(ff6) )\n"
                   /*
                   * Post-Tag
                   */
                   + "FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG + "]->(tag4) )\n"
                   + "FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:" + Rels.POST_HAS_TAG + "]->(tag5) )\n"
                   + "FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:" +
                   Rels.POST_HAS_TAG + "]->(tag3) )\n"
                   + "FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG +
                   "]->(tag2) )\n"
                   + "FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:" + Rels.POST_HAS_TAG +
                   "]->(tag1) )\n";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "tag1", TestTags.tag1(),
                    "tag2", TestTags.tag2(),
                    "tag3", TestTags.tag3(),
                    "tag4", TestTags.tag4(),
                    "tag5", TestTags.tag5(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1() );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1@email.com"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"language1"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"language2"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f3@email.com"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"language3"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4" );
                params.put( Person.CREATION_DATE, 4L );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"} );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.LANGUAGES, new String[]{"language4"} );
                params.put( Person.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> s5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "s5" );
                params.put( Person.LAST_NAME, "last5" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "browser5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f5@email.com"} );
                params.put( Person.GENDER, "gender5" );
                params.put( Person.LANGUAGES, new String[]{"language5"} );
                params.put( Person.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6" );
                params.put( Person.CREATION_DATE, 6L );
                params.put( Person.BIRTHDAY, 6L );
                params.put( Person.BROWSER_USED, "browser6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"} );
                params.put( Person.GENDER, "gender6" );
                params.put( Person.LANGUAGES, new String[]{"language6"} );
                params.put( Person.LOCATION_IP, "ip6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "browser7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"} );
                params.put( Person.GENDER, "gender7" );
                params.put( Person.LANGUAGES, new String[]{"language7"} );
                params.put( Person.LOCATION_IP, "ip7" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> f3Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "[f3Post1] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "[f3Post2] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f3Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "[f3Post3] content" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "image3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CONTENT, "[f4Post1] content" );
                params.put( Post.LANGUAGE, "language4" );
                params.put( Post.IMAGE_FILE, "image4" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser4" );
                params.put( Message.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> f2Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "[f2Post1] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "[f2Post2] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f2Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 7L );
                params.put( Message.CONTENT, "[f2Post3] content" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "image2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> s5Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 8L );
                params.put( Message.CONTENT, "[s5Post1] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s5Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 9L );
                params.put( Message.CONTENT, "[s5Post2] content" );
                params.put( Post.LANGUAGE, "language5" );
                params.put( Post.IMAGE_FILE, "image5" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CONTENT, "[s7Post1] content" );
                params.put( Post.LANGUAGE, "language7" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> s7Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "[s7Post2] content" );
                params.put( Post.LANGUAGE, "language7" );
                params.put( Post.IMAGE_FILE, "image7" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser7" );
                params.put( Message.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> ff6Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "[ff6Post1] content" );
                params.put( Post.LANGUAGE, "language6" );
                params.put( Post.IMAGE_FILE, "image6" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 3 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }

        protected static class TestTags
        {
            protected static Map<String,Object> tag1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag1-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag2-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag3-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag4-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> tag5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag5-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }
    }

    public static class Query7GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + "(person1:" + Nodes.Person + " $person1),\n"
                   + "(f2:" + Nodes.Person + " $f2),\n"
                   + "(f3:" + Nodes.Person + " $f3),\n"
                   + "(f4:" + Nodes.Person + " $f4),\n"
                   + "(ff5:" + Nodes.Person + " $ff5),\n"
                   + "(ff6:" + Nodes.Person + " $ff6),\n"
                   + "(s7:" + Nodes.Person + " $s7),\n"
                   + "(s8:" + Nodes.Person + " $s8),\n"
                   /*
                    * Posts
                    */
                   + "(person1Post1:" + Nodes.Post + ":" + Nodes.Message + " $person1Post1),\n"
                   + "(person1Post2:" + Nodes.Post + ":" + Nodes.Message + " $person1Post2),\n"
                   + "(person1Post3:" + Nodes.Post + ":" + Nodes.Message + " $person1Post3),\n"
                   + "(s7Post1:" + Nodes.Post + ":" + Nodes.Message + " $s7Post1),\n"
                   /*
                   * Cities
                   */
                   + " (city0:" + Place.Type.City + " $city0),\n"
                   /*
                    * Comments
                    */
                   + "(person1Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $person1Comment1),\n"
                   + "(f4Comment1:" + Nodes.Comment + ":" + Nodes.Message + " $f4Comment1),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person1)-[:" + Rels.KNOWS + "]->(f2),\n"
                   + "(person1)-[:" + Rels.KNOWS + "]->(f3),\n"
                   + "(person1)-[:" + Rels.KNOWS + "]->(f4),\n"
                   + "(f2)-[:" + Rels.KNOWS + "]->(ff5),\n"
                   + "(f3)-[:" + Rels.KNOWS + "]->(ff6),\n"
                   /*
                   * Person-City
                   */
                   + " (person1)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f2)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f3)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f4)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff5)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff6)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (s7)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (s8)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   /*
                    * Comment-Post
                    */
                   + "(person1Comment1)-[:" + Rels.REPLY_OF_POST + "]->(s7Post1),\n"
                   + "(f4Comment1)-[:" + Rels.REPLY_OF_POST + "]->(person1Post3),\n"
                   /*
                    * Person-Like->Post
                    */
                   + "(person1)-[:" + Rels.LIKES_POST + " $person1LikesPerson1Post1]->(person1Post1),\n"
                   + "(person1)-[:" + Rels.LIKES_POST + " $person1LikesS7Post1]->(s7Post1),\n"
                   + "(f2)-[:" + Rels.LIKES_POST + " $f2LikesPerson1Post1]->(person1Post1),\n"
                   + "(f4)-[:" + Rels.LIKES_POST + " $f4LikesPerson1Post1]->(person1Post1),\n"
                   + "(f4)-[:" + Rels.LIKES_POST + " $f4LikesPerson1Post2]->(person1Post2),\n"
                   + "(f4)-[:" + Rels.LIKES_POST + " $f4LikesPerson1Post3]->(person1Post3),\n"
                   + "(ff6)-[:" + Rels.LIKES_POST + " $ff6OldLikesPerson1Post1]->(person1Post1),\n"
                   + "(ff6)-[:" + Rels.LIKES_POST + " $ff6NewLikesPerson1Post1]->(person1Post1),\n"
                   + "(ff6)-[:" + Rels.LIKES_POST + " $ff6LikesPerson1Post2]->(person1Post2),\n"
                   + "(s7)-[:" + Rels.LIKES_POST + " $s7LikesPerson1Post1]->(person1Post1),\n"
                   + "(s8)-[:" + Rels.LIKES_POST + " $s8LikesPerson1Post2]->(person1Post2),\n"
                   /*
                    * Person-Like->Comment
                    */
                   + "(person1)-[:" + Rels.LIKES_COMMENT + " $person1LikesF4Comment1]->(f4Comment1),\n"
                   + "(s7)-[:" + Rels.LIKES_COMMENT + " $s7LikesPerson1Comment1]->(person1Comment1),\n"
                   + "(s8)-[:" + Rels.LIKES_COMMENT + " $s8LikesF4Comment1]->(f4Comment1),\n"
                   /*
                    * Post-Create->Person
                    */
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(person1Post1),\n"
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(person1Post2),\n"
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(person1Post3),\n"
                   + "(s7)<-[:" + Rels.POST_HAS_CREATOR + "]-(s7Post1),\n"
                   /*
                    * Comment-Person
                    */
                   + "(person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(person1Comment1),\n"
                   + "(f4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(f4Comment1)\n";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "ff5", TestPersons.ff5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "s8", TestPersons.s8(),
                    // Cities
                    "city0", TestCities.city0(),
                    // Person-Post (like)
                    "person1LikesPerson1Post1", TestPostLikes.person1LikesPerson1Post1(),
                    "person1LikesS7Post1", TestPostLikes.person1LikesS7Post1(),
                    "f2LikesPerson1Post1", TestPostLikes.f2LikesPerson1Post1(),
                    "f4LikesPerson1Post1", TestPostLikes.f4LikesPerson1Post1(),
                    "f4LikesPerson1Post2", TestPostLikes.f4LikesPerson1Post2(),
                    "f4LikesPerson1Post3", TestPostLikes.f4LikesPerson1Post3(),
                    "ff6OldLikesPerson1Post1", TestPostLikes.ff6OldLikesPerson1Post1(),
                    "ff6NewLikesPerson1Post1", TestPostLikes.ff6NewLikesPerson1Post1(),
                    "ff6LikesPerson1Post2", TestPostLikes.ff6LikesPerson1Post2(),
                    "s7LikesPerson1Post1", TestPostLikes.s7LikesPerson1Post1(),
                    "s8LikesPerson1Post2", TestPostLikes.s8LikesPerson1Post2(),
                    //Person-Comment (like)
                    "person1LikesF4Comment1", TestCommentLikes.person1LikesF4Comment1(),
                    "s7LikesPerson1Comment1", TestCommentLikes.s7LikesPerson1Comment1(),
                    "s8LikesF4Comment1", TestCommentLikes.s8LikesF4Comment1(),
                    // Posts
                    "person1Post1", TestPosts.person1Post1(),
                    "person1Post2", TestPosts.person1Post2(),
                    "person1Post3", TestPosts.person1Post3(),
                    "s7Post1", TestPosts.s7Post1(),
                    // Comments
                    "person1Comment1", TestComments.person1Comment1(),
                    "f4Comment1", TestComments.f4Comment1()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "last1-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 1L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"person1@email.com"} );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.LANGUAGES, new String[]{"language1"} );
                params.put( Person.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f2" );
                params.put( Person.LAST_NAME, "last2-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 2L );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"} );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.LANGUAGES, new String[]{"language2"} );
                params.put( Person.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f3" );
                params.put( Person.LAST_NAME, "last3-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 3L );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f3@email.com"} );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.LANGUAGES, new String[]{"language3"} );
                params.put( Person.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f4" );
                params.put( Person.LAST_NAME, "last4-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 4L );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"} );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.LANGUAGES, new String[]{"language4"} );
                params.put( Person.LOCATION_IP, "ip4" );
                return params;
            }

            protected static Map<String,Object> ff5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 5L );
                params.put( Person.FIRST_NAME, "ff5" );
                params.put( Person.LAST_NAME, "last5-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 5L );
                params.put( Person.BIRTHDAY, 5L );
                params.put( Person.BROWSER_USED, "browser5" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff5@email.com"} );
                params.put( Person.GENDER, "gender5" );
                params.put( Person.LANGUAGES, new String[]{"language5"} );
                params.put( Person.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> ff6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 6L );
                params.put( Person.FIRST_NAME, "ff6" );
                params.put( Person.LAST_NAME, "last6-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 6L );
                params.put( Person.BIRTHDAY, 6L );
                params.put( Person.BROWSER_USED, "browser6" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"} );
                params.put( Person.GENDER, "gender6" );
                params.put( Person.LANGUAGES, new String[]{"language6"} );
                params.put( Person.LOCATION_IP, "ip6" );
                return params;
            }

            protected static Map<String,Object> s7()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 7L );
                params.put( Person.FIRST_NAME, "s7" );
                params.put( Person.LAST_NAME, "last7-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 7L );
                params.put( Person.BIRTHDAY, 7L );
                params.put( Person.BROWSER_USED, "browser7" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"} );
                params.put( Person.GENDER, "gender7" );
                params.put( Person.LANGUAGES, new String[]{"language7"} );
                params.put( Person.LOCATION_IP, "ip7" );
                return params;
            }

            protected static Map<String,Object> s8()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 8L );
                params.put( Person.FIRST_NAME, "s8" );
                params.put( Person.LAST_NAME, "last8-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.CREATION_DATE, 8L );
                params.put( Person.BIRTHDAY, 8L );
                params.put( Person.BROWSER_USED, "browser8" );
                params.put( Person.EMAIL_ADDRESSES, new String[]{"s8@email.com"} );
                params.put( Person.GENDER, "gender8" );
                params.put( Person.LANGUAGES, new String[]{"language8"} );
                params.put( Person.LOCATION_IP, "ip8" );
                return params;
            }
        }

        protected static class TestCities
        {
            protected static Map<String,Object> city0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 0L );
                params.put( Place.NAME, "city0" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> person1Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "person1post1" );
                params.put( Post.LANGUAGE, "language1" );
                params.put( Post.IMAGE_FILE, "imageFile1" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 1, 0 ) );
                params.put( Message.BROWSER_USED, "browser1" );
                params.put( Message.LOCATION_IP, "ip1" );
                return params;
            }

            protected static Map<String,Object> person1Post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "person1post2" );
                params.put( Post.LANGUAGE, "language2" );
                params.put( Post.IMAGE_FILE, "imageFile2" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 2, 0 ) );
                params.put( Message.BROWSER_USED, "browser2" );
                params.put( Message.LOCATION_IP, "ip2" );
                return params;
            }

            protected static Map<String,Object> person1Post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "person1post3" );
                params.put( Post.LANGUAGE, "language3" );
                params.put( Post.IMAGE_FILE, "imageFile3" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 3, 0 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }

            protected static Map<String,Object> s7Post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CONTENT, "s7post1" );
                params.put( Post.LANGUAGE, "language4" );
                params.put( Post.IMAGE_FILE, "imageFile4" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 4, 0 ) );
                params.put( Message.BROWSER_USED, "browser3" );
                params.put( Message.LOCATION_IP, "ip3" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> person1Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CONTENT, "person1comment1" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 5, 0 ) );
                params.put( Message.BROWSER_USED, "browser5" );
                params.put( Message.LOCATION_IP, "ip5" );
                return params;
            }

            protected static Map<String,Object> f4Comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 6L );
                params.put( Message.CONTENT, "f4comment1" );
                params.put( Message.CREATION_DATE, date( 2000, 1, 1, 0, 6, 0 ) );
                params.put( Message.BROWSER_USED, "browser6" );
                params.put( Message.LOCATION_IP, "ip6" );
                return params;
            }
        }

        protected static class TestPostLikes
        {
            protected static Map<String,Object> person1LikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 1, 0 ) );
            }

            protected static Map<String,Object> person1LikesS7Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 20, 0 ) );
            }

            protected static Map<String,Object> f2LikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 5, 0 ) );
            }

            protected static Map<String,Object> f4LikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 3, 0 ) );
            }

            protected static Map<String,Object> f4LikesPerson1Post2()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 4, 0 ) );
            }

            protected static Map<String,Object> f4LikesPerson1Post3()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 5, 0 ) );
            }

            protected static Map<String,Object> ff6OldLikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 2, 0 ) );
            }

            protected static Map<String,Object> ff6NewLikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 4, 0 ) );
            }

            protected static Map<String,Object> ff6LikesPerson1Post2()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 3, 0 ) );
            }

            protected static Map<String,Object> s7LikesPerson1Post1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 2, 0 ) );
            }

            protected static Map<String,Object> s8LikesPerson1Post2()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 10, 0 ) );
            }
        }

        protected static class TestCommentLikes
        {
            protected static Map<String,Object> person1LikesF4Comment1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 20, 0 ) );
            }

            protected static Map<String,Object> s7LikesPerson1Comment1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 6, 0 ) );
            }

            protected static Map<String,Object> s8LikesF4Comment1()
            {
                return MapUtil.map( Likes.CREATION_DATE, date( 2000, 1, 1, 0, 20, 0 ) );
            }
        }
    }

    public static class Query8GraphMaker extends QueryGraphMaker
    {

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person:" + Nodes.Person + " $person),\n"
                   + " (friend1:" + Nodes.Person + " $friend1),\n"
                   + " (friend2:" + Nodes.Person + " $friend2),\n"
                   + " (friend3:" + Nodes.Person + " $friend3),\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " $post2),\n"
                   + " (post3:" + Nodes.Post + ":" + Nodes.Message + " $post3),\n"
                   /*
                    * Comments
                    */
                   + " (comment01:" + Nodes.Comment + ":" + Nodes.Message + " $comment01),\n"
                   + " (comment11:" + Nodes.Comment + ":" + Nodes.Message + " $comment11),\n"
                   + " (comment12:" + Nodes.Comment + ":" + Nodes.Message + " $comment12),\n"
                   + " (comment13:" + Nodes.Comment + ":" + Nodes.Message + " $comment13),\n"
                   + " (comment131:" + Nodes.Comment + ":" + Nodes.Message + " $comment131),\n"
                   + " (comment111:" + Nodes.Comment + ":" + Nodes.Message + " $comment111),\n"
                   + " (comment112:" + Nodes.Comment + ":" + Nodes.Message + " $comment112),\n"
                   + " (comment21:" + Nodes.Comment + ":" + Nodes.Message + " $comment21),\n"
                   + " (comment211:" + Nodes.Comment + ":" + Nodes.Message + " $comment211),\n"
                   + " (comment2111:" + Nodes.Comment + ":" + Nodes.Message + " $comment2111),\n"
                   + " (comment31:" + Nodes.Comment + ":" + Nodes.Message + " $comment31),\n"
                   + " (comment32:" + Nodes.Comment + ":" + Nodes.Message + " $comment32),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Post
                    */
                   + "(person)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + "(person)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + "(person)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   + "(friend3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post3),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment13),\n"
                   + "(person)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment31),\n"
                   + "(friend1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment111),\n"
                   + "(friend1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment21),\n"
                   + "(friend1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment2111),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment211),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment131),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment112),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment32),\n"
                   + "(friend3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment11),\n"
                   + "(friend3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment12),\n"
                   + "(friend3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment01),\n"
                   /*
                    * Comment-Post/Comment
                    */
                   + "(post0)<-[:" + Rels.REPLY_OF_POST + "]-(comment01),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment11)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment111), (comment11)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment112),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment12),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment13)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment131),\n"
                   + "(post2)<-[:" + Rels.REPLY_OF_POST + "]-(comment21)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment211)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment2111),\n"
                   + "(post3)<-[:" + Rels.REPLY_OF_POST + "]-(comment31), (post3)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment32)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person", TestPersons.person(),
                    "friend1", TestPersons.friend1(),
                    "friend2", TestPersons.friend2(),
                    "friend3", TestPersons.friend3(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    "post2", TestPosts.post2(),
                    "post3", TestPosts.post3(),
                    // Comments
                    "comment01", TestComments.comment01(),
                    "comment11", TestComments.comment11(),
                    "comment12", TestComments.comment12(),
                    "comment13", TestComments.comment13(),
                    "comment131", TestComments.comment131(),
                    "comment111", TestComments.comment111(),
                    "comment112", TestComments.comment112(),
                    "comment21", TestComments.comment21(),
                    "comment211", TestComments.comment211(),
                    "comment2111", TestComments.comment2111(),
                    "comment31", TestComments.comment31(),
                    "comment32", TestComments.comment32()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friend1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friend2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friend3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "three-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CONTENT, "P0" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "P1" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CONTENT, "P2" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CONTENT, "P3" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment01()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.CONTENT, "C01" );
                return params;
            }

            protected static Map<String,Object> comment11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.CONTENT, "C11" );
                return params;
            }

            protected static Map<String,Object> comment12()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.CONTENT, "C12" );
                return params;
            }

            protected static Map<String,Object> comment13()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 13L );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.CONTENT, "C13" );
                return params;
            }

            protected static Map<String,Object> comment131()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 14L );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.CONTENT, "C131" );
                return params;
            }

            protected static Map<String,Object> comment111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 15L );
                params.put( Message.CREATION_DATE, 5L );
                params.put( Message.CONTENT, "C111" );
                return params;
            }

            protected static Map<String,Object> comment112()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 16L );
                params.put( Message.CREATION_DATE, 6L );
                params.put( Message.CONTENT, "C112" );
                return params;
            }

            protected static Map<String,Object> comment21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 17L );
                params.put( Message.CREATION_DATE, 7L );
                params.put( Message.CONTENT, "C21" );
                return params;
            }

            protected static Map<String,Object> comment211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 18L );
                params.put( Message.CREATION_DATE, 8L );
                params.put( Message.CONTENT, "C211" );
                return params;
            }

            protected static Map<String,Object> comment2111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 19L );
                params.put( Message.CREATION_DATE, 9L );
                params.put( Message.CONTENT, "C2111" );
                return params;
            }

            protected static Map<String,Object> comment31()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 20L );
                params.put( Message.CREATION_DATE, 10L );
                params.put( Message.CONTENT, "C31" );
                return params;
            }

            protected static Map<String,Object> comment32()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 21L );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.CONTENT, "C32" );
                return params;
            }
        }
    }

    public static class Query9GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (friend1:" + Nodes.Person + " $friend1),\n"
                   + " (friend2:" + Nodes.Person + " $friend2),\n"
                   + " (stranger3:" + Nodes.Person + " $stranger3),\n"
                   + " (friendfriend4:" + Nodes.Person + " $friendfriend4),\n"
                   /*
                    * Posts
                    */
                   + " (post01:" + Nodes.Post + ":" + Nodes.Message + " $post01),\n"
                   + " (post11:" + Nodes.Post + ":" + Nodes.Message + " $post11),\n"
                   + " (post12:" + Nodes.Post + ":" + Nodes.Message + " $post12),\n"
                   + " (post21:" + Nodes.Post + ":" + Nodes.Message + " $post21),\n"
                   + " (post31:" + Nodes.Post + ":" + Nodes.Message + " $post31),\n"
                   /*
                    * Comments
                    */
                   + " (comment111:" + Nodes.Comment + ":" + Nodes.Message + " $comment111),\n"
                   + " (comment121:" + Nodes.Comment + ":" + Nodes.Message + " $comment121),\n"
                   + " (comment1211:" + Nodes.Comment + ":" + Nodes.Message + " $comment1211),\n"
                   + " (comment211:" + Nodes.Comment + ":" + Nodes.Message + " $comment211),\n"
                   + " (comment2111:" + Nodes.Comment + ":" + Nodes.Message + " $comment2111),\n"
                   + " (comment21111:" + Nodes.Comment + ":" + Nodes.Message + " $comment21111),\n"
                   + " (comment311:" + Nodes.Comment + ":" + Nodes.Message + " $comment311),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person0)-[:" + Rels.KNOWS + "]->(friend1)-[:" + Rels.KNOWS + "]->(friendfriend4)<-[:" +
                   Rels.KNOWS + "]-(friend2),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(friend2)<-[:" + Rels.KNOWS + "]-(friend1),\n"
                   /*
                    * Person-Post
                    */
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post01),\n"
                   + "(friend1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post11),\n"
                   + "(friend1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post12),\n"
                   + "(friend2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post21),\n"
                   + "(stranger3)<-[:" + Rels.POST_HAS_CREATOR + "]-(post31),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment111),\n"
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment121),\n"
                   + "(friend1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment211),\n"
                   + "(friend1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment21111),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment2111),\n"
                   + "(friend2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment311),\n"
                   + "(friendfriend4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment1211),\n"
                   /*
                    * Comment-Post/Comment
                    */
                   + "(post11)<-[:" + Rels.REPLY_OF_POST + "]-(comment111),\n"
                   + "(post12)<-[:" + Rels.REPLY_OF_POST + "]-(comment121)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment1211),\n"
                   + "(post21)<-[:" + Rels.REPLY_OF_POST + "]-(comment211)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment2111)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment21111),\n"
                   + "(post31)<-[:" + Rels.REPLY_OF_POST + "]-(comment311)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "friend1", TestPersons.friend1(),
                    "friend2", TestPersons.friend2(),
                    "stranger3", TestPersons.stranger3(),
                    "friendfriend4", TestPersons.friendfriend4(),
                    // Posts
                    "post01", TestPosts.post01(),
                    "post11", TestPosts.post11(),
                    "post12", TestPosts.post12(),
                    "post21", TestPosts.post21(),
                    "post31", TestPosts.post31(),
                    // Comments
                    "comment111", TestComments.comment111(),
                    "comment121", TestComments.comment121(),
                    "comment1211", TestComments.comment1211(),
                    "comment211", TestComments.comment211(),
                    "comment2111", TestComments.comment2111(),
                    "comment21111", TestComments.comment21111(),
                    "comment311", TestComments.comment311()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friend1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friend2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> stranger3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "stranger" );
                params.put( Person.LAST_NAME, "three-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> friendfriend4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "four-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post01()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CONTENT, "P01 - content" );
                params.put( Post.IMAGE_FILE, "P01 - image" );
                params.put( Message.CREATION_DATE, 3L );
                return params;
            }

            protected static Map<String,Object> post11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CONTENT, "P11 - content" );
                params.put( Post.IMAGE_FILE, "P11 - image" );
                params.put( Message.CREATION_DATE, 11L );
                return params;
            }

            protected static Map<String,Object> post12()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CONTENT, "P12 - content" );
                params.put( Post.IMAGE_FILE, "P12 - image" );
                params.put( Message.CREATION_DATE, 4L );
                return params;
            }

            protected static Map<String,Object> post21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 21L );
                params.put( Post.IMAGE_FILE, "P21 - image" );
                params.put( Message.CREATION_DATE, 6L );
                return params;
            }

            protected static Map<String,Object> post31()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 31L );
                params.put( Message.CONTENT, "P31 - content" );
                params.put( Post.IMAGE_FILE, "P31 - image" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 111L );
                params.put( Message.CREATION_DATE, 12L );
                params.put( Message.CONTENT, "C111" );
                return params;
            }

            protected static Map<String,Object> comment121()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 121L );
                params.put( Message.CREATION_DATE, 5L );
                params.put( Message.CONTENT, "C121" );
                return params;
            }

            protected static Map<String,Object> comment1211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1211L );
                params.put( Message.CREATION_DATE, 10L );
                params.put( Message.CONTENT, "C1211" );
                return params;
            }

            protected static Map<String,Object> comment211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 211L );
                params.put( Message.CREATION_DATE, 7L );
                params.put( Message.CONTENT, "C211" );
                return params;
            }

            protected static Map<String,Object> comment2111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2111L );
                params.put( Message.CREATION_DATE, 8L );
                params.put( Message.CONTENT, "C2111" );
                return params;
            }

            protected static Map<String,Object> comment21111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 21111L );
                params.put( Message.CREATION_DATE, 9L );
                params.put( Message.CONTENT, "C21111" );
                return params;
            }

            protected static Map<String,Object> comment311()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 311L );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.CONTENT, "C311" );
                return params;
            }
        }
    }

    public static class Query10GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (f1:" + Nodes.Person + " $f1),\n"
                   + " (f2:" + Nodes.Person + " $f2),\n"
                   + " (ff11:" + Nodes.Person + " $ff11),\n"
                   + " (ff12:" + Nodes.Person + " $ff12),\n"
                   + " (ff21:" + Nodes.Person + " $ff21),\n"
                   + " (ff22:" + Nodes.Person + " $ff22),\n"
                   + " (ff23:" + Nodes.Person + " $ff23),\n"
                   /*
                    * Posts
                    */
                   + " (post21:" + Nodes.Post + ":" + Nodes.Message + " $post21),\n"
                   + " (post111:" + Nodes.Post + ":" + Nodes.Message + " $post111),\n"
                   + " (post112:" + Nodes.Post + ":" + Nodes.Message + " $post112),\n"
                   + " (post113:" + Nodes.Post + ":" + Nodes.Message + " $post113),\n"
                   + " (post121:" + Nodes.Post + ":" + Nodes.Message + " $post121),\n"
                   + " (post211:" + Nodes.Post + ":" + Nodes.Message + " $post211),\n"
                   + " (post212:" + Nodes.Post + ":" + Nodes.Message + " $post212),\n"
                   + " (post213:" + Nodes.Post + ":" + Nodes.Message + " $post213),\n"
                   /*
                    * Cities
                    */
                   + " (city0:" + Place.Type.City + " $city0),\n"
                   + " (city1:" + Place.Type.City + " $city1),\n"
                   /*
                    * Tags
                    */
                   + " (uncommonTag1:" + Nodes.Tag + " $uncommonTag1),\n"
                   + " (uncommonTag2:" + Nodes.Tag + " $uncommonTag2),\n"
                   + " (uncommonTag3:" + Nodes.Tag + " $uncommonTag3),\n"
                   + " (commonTag4:" + Nodes.Tag + " $commonTag4),\n"
                   + " (commonTag5:" + Nodes.Tag + " $commonTag5),\n"
                   + " (commonTag6:" + Nodes.Tag + " $commonTag6),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person0)-[:" + Rels.KNOWS + "]->(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   + "(ff11)<-[:" + Rels.KNOWS + "]-(f2)<-[:" + Rels.KNOWS + "]-(f1)-[:" + Rels.KNOWS + "]->(ff12),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(f2)-[:" + Rels.KNOWS + "]->(ff21),\n"
                   + "(f2)-[:" + Rels.KNOWS + "]->(ff22),\n"
                   + "(f2)-[:" + Rels.KNOWS + "]->(ff23),\n"
                   /*
                    * Person-City
                    */
                   + " (person0)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f1)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (f2)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff11)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + " (ff12)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff21)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff22)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + " (ff23)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   /*
                    * Person-Post
                    */
                   + "(f2)<-[:" + Rels.POST_HAS_CREATOR + "]-(post21),\n"
                   + "(ff11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post111),\n"
                   + "(ff11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post112),\n"
                   + "(ff11)<-[:" + Rels.POST_HAS_CREATOR + "]-(post113),\n"
                   + "(ff12)<-[:" + Rels.POST_HAS_CREATOR + "]-(post121),\n"
                   + "(ff21)<-[:" + Rels.POST_HAS_CREATOR + "]-(post211),\n"
                   + "(ff21)<-[:" + Rels.POST_HAS_CREATOR + "]-(post212),\n"
                   + "(ff21)<-[:" + Rels.POST_HAS_CREATOR + "]-(post213),\n"
                   /*
                    * Person-Tag
                    */
                   + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag4),\n"
                   + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag5),\n"
                   + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag6),\n"
                   /*
                    * Post-Tag
                    */
                   + "(post21)-[:" + Rels.POST_HAS_TAG + "]->(commonTag4),\n"
                   + "(post111)-[:" + Rels.POST_HAS_TAG + "]->(uncommonTag2),\n"
                   + "(post112)-[:" + Rels.POST_HAS_TAG + "]->(uncommonTag2),\n"
                   + "(post113)-[:" + Rels.POST_HAS_TAG + "]->(commonTag5),\n"
                   + "(post113)-[:" + Rels.POST_HAS_TAG + "]->(commonTag6),\n"
                   + "(post211)-[:" + Rels.POST_HAS_TAG + "]->(uncommonTag1),\n"
                   + "(post212)-[:" + Rels.POST_HAS_TAG + "]->(uncommonTag3),\n"
                   + "(post212)-[:" + Rels.POST_HAS_TAG + "]->(commonTag4),\n"
                   + "(post213)-[:" + Rels.POST_HAS_TAG + "]->(uncommonTag3)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "ff11", TestPersons.ff11(),
                    "ff12", TestPersons.ff12(),
                    "ff21", TestPersons.ff21(),
                    "ff22", TestPersons.ff22(),
                    "ff23", TestPersons.ff23(),
                    // Posts
                    "post21", TestPosts.post21(),
                    "post111", TestPosts.post111(),
                    "post112", TestPosts.post112(),
                    "post113", TestPosts.post113(),
                    "post121", TestPosts.post121(),
                    "post211", TestPosts.post211(),
                    "post212", TestPosts.post212(),
                    "post213", TestPosts.post213(),
                    // Cities
                    "city0", TestCities.city0(),
                    "city1", TestCities.city1(),
                    // Tags
                    "uncommonTag1", TestTags.uncommonTag1(),
                    "uncommonTag2", TestTags.uncommonTag2(),
                    "uncommonTag3", TestTags.uncommonTag3(),
                    "commonTag4", TestTags.commonTag4(),
                    "commonTag5", TestTags.commonTag5(),
                    "commonTag6", TestTags.commonTag6()
            );
        }

        protected static class TestCities
        {
            protected static Map<String,Object> city0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.NAME, "city0" );
                return params;
            }

            protected static Map<String,Object> city1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.NAME, "city1" );
                return params;
            }
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 6;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> f1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> ff11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 11L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "one one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "female" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> ff12()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 12L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "one two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> ff21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 21L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "two one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> ff22()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 22L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "two two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 2;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }

            protected static Map<String,Object> ff23()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 23L );
                params.put( Person.FIRST_NAME, "friendfriend" );
                params.put( Person.LAST_NAME, "two three-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.GENDER, "male" );
                int year = 2010;
                int month = 3;
                int day = 1;
                long dateAsUtc = date( year, month, day );
                params.put( Person.BIRTHDAY, dateAsUtc );
                params.put( Person.BIRTHDAY_MONTH, month );
                params.put( Person.BIRTHDAY_DAY_OF_MONTH, day );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 21L );
                params.put( Message.CONTENT, "P21" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 111L );
                params.put( Message.CONTENT, "P111" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post112()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 112L );
                params.put( Message.CONTENT, "P112" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post113()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 113L );
                params.put( Message.CONTENT, "P113" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post121()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 121L );
                params.put( Message.CONTENT, "P121" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 211L );
                params.put( Message.CONTENT, "P211" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post212()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 212L );
                params.put( Message.CONTENT, "P212" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> post213()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 213L );
                params.put( Message.CONTENT, "P213" );
                params.put( Message.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestTags
        {
            protected static Map<String,Object> uncommonTag1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "uncommon tag 1" );
                return params;
            }

            protected static Map<String,Object> uncommonTag2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "uncommon tag 2" );
                return params;
            }

            protected static Map<String,Object> uncommonTag3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "common tag 3" );
                return params;
            }

            protected static Map<String,Object> commonTag4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "common tag 4" );
                return params;
            }

            protected static Map<String,Object> commonTag5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "common tag 5" );
                return params;
            }

            protected static Map<String,Object> commonTag6()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "common tag 6" );
                return params;
            }
        }
    }

    public static class Query11GraphMaker extends QueryGraphMaker
    {

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (f1:" + Nodes.Person + " $f1),\n"
                   + " (f2:" + Nodes.Person + " $f2),\n"
                   + " (stranger3:" + Nodes.Person + " $stranger3),\n"
                   + " (ff11:" + Nodes.Person + " $ff11),\n"
                   /*
                    * Companies
                    */
                   + " (company0:" + Organisation.Type.Company + " $company0),\n"
                   + " (company1:" + Organisation.Type.Company + " $company1),\n"
                   + " (company2:" + Organisation.Type.Company + " $company2),\n"
                   /*
                    * Countries
                    */
                   + " (country0:" + Place.Type.Country + " $country0),\n"
                   + " (country1:" + Place.Type.Country + " $country1),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person0)-[:" + Rels.KNOWS + "]->(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   + " (f1)-[:" + Rels.KNOWS + "]->(f2),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                   /*
                    * Person-Company
                    */
                   + "(f1)-[:" + Rels.WORKS_AT + " $f1WorkedAtCompany0]->(company0),\n"
                   + "(f1)-[:" + Rels.WORKS_AT + " $f1WorkedAtCompany1]->(company1),\n"
                   + "(f2)-[:" + Rels.WORKS_AT + " $f2WorkedAtCompany2]->(company2),\n"
                   + "(ff11)-[:" + Rels.WORKS_AT + " $ff11WorkedAtCompany0]->(company0),\n"
                   + "(stranger3)-[:" + Rels.WORKS_AT + " $stranger3WorkedAtCompany2]->(company2),\n"
                   /*
                    * Company-Country
                    */
                   + "(company0)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + "]->(country0),\n"
                   + "(company1)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + "]->(country1),\n"
                   + "(company2)-[:" + Rels.ORGANISATION_IS_LOCATED_IN + "]->(country0)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "stranger3", TestPersons.stranger3(),
                    "ff11", TestPersons.ff11(),
                    // Companies
                    "company0", TestCompanies.company0(),
                    "company1", TestCompanies.company1(),
                    "company2", TestCompanies.company2(),
                    // -WorkedAt-
                    "f1WorkedAtCompany0", TestWorkedAt.f1WorkedAtCompany0(),
                    "f1WorkedAtCompany1", TestWorkedAt.f1WorkedAtCompany1(),
                    "f2WorkedAtCompany2", TestWorkedAt.f2WorkedAtCompany2(),
                    "ff11WorkedAtCompany0", TestWorkedAt.ff11WorkedAtCompany0(),
                    "stranger3WorkedAtCompany2", TestWorkedAt.stranger3WorkedAtCompany2(),
                    // Countries
                    "country0", TestCountries.country0(),
                    "country1", TestCountries.country1()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> f1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "friend" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> stranger3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "stranger" );
                params.put( Person.LAST_NAME, "three-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> ff11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 11L );
                params.put( Person.FIRST_NAME, "friend friend" );
                params.put( Person.LAST_NAME, "one one-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }

        protected static class TestCompanies
        {
            protected static Map<String,Object> company0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.NAME, "company zero" );
                return params;
            }

            protected static Map<String,Object> company1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.NAME, "company one" );
                return params;
            }

            protected static Map<String,Object> company2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Organisation.NAME, "company two" );
                return params;
            }
        }

        protected static class TestWorkedAt
        {
            protected static Map<String,Object> f1WorkedAtCompany0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 2 );
                return params;
            }

            protected static Map<String,Object> f1WorkedAtCompany1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 4 );
                return params;
            }

            protected static Map<String,Object> f2WorkedAtCompany2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 5 );
                return params;
            }

            protected static Map<String,Object> ff11WorkedAtCompany0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 3 );
                return params;
            }

            protected static Map<String,Object> stranger3WorkedAtCompany2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( WorksAt.WORK_FROM, 1 );
                return params;
            }
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.NAME, "country0" );
                return params;
            }

            protected static Map<String,Object> country1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.NAME, "country1" );
                return params;
            }
        }
    }

    public static class Query12GraphMaker extends QueryGraphMaker
    {

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (f1:" + Nodes.Person + " $f1),\n"
                   + " (f2:" + Nodes.Person + " $f2),\n"
                   + " (f3:" + Nodes.Person + " $f3),\n"
                   + " (f4:" + Nodes.Person + " $f4),\n"
                   + " (ff11:" + Nodes.Person + " $ff11),\n"
                   /*
                    * TagClass
                    */
                   + " (tc1:" + Nodes.TagClass + " $tc1),\n"
                   + " (tc11:" + Nodes.TagClass + " $tc11),\n"
                   + " (tc12:" + Nodes.TagClass + " $tc12),\n"
                   + " (tc121:" + Nodes.TagClass + " $tc121),\n"
                   + " (tc1211:" + Nodes.TagClass + " $tc1211),\n"
                   + " (tc2:" + Nodes.TagClass + " $tc2),\n"
                   + " (tc21:" + Nodes.TagClass + " $tc21),\n"
                   /*
                    * Tag
                    */
                   + " (t11:" + Nodes.Tag + " $t11),\n"
                   + " (t111:" + Nodes.Tag + " $t111),\n"
                   + " (t112:" + Nodes.Tag + " $t112),\n"
                   + " (t12111:" + Nodes.Tag + " $t12111),\n"
                   + " (t21:" + Nodes.Tag + " $t21),\n"
                   + " (t211:" + Nodes.Tag + " $t211),\n"
                   /*
                    * Post
                    */
                   + " (p11:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p11'," + Message.CREATION_DATE + ":1}),\n"
                   + " (p111:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p111'," + Message.CREATION_DATE + ":1}),\n"
                   + " (p112:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p112'," + Message.CREATION_DATE + ":1}),\n"
                   + " (p12111:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p12111'," + Message.CREATION_DATE + ":1}),\n"
                   + " (p21:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p21'," + Message.CREATION_DATE + ":1}),\n"
                   + " (p211:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.CONTENT + ":'p211'," + Message.CREATION_DATE + ":1}),\n"
                   /*
                    * Comment
                    */
                   + " (c111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c1111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c1111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c11111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c11111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c111111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c111111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c11112:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c11112',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c1112:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c1112',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c1121:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c1121',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c11211:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c11211',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c112111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c112111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c112112:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c112112',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c121111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c121111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c211:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c211',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"
                   + " (c2111:" + Nodes.Comment + ":" + Nodes.Message + " {"
                   + Message.CONTENT + ":'c2111',"
                   + Message.CREATION_DATE + ":1"
                   + "}),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person0)-[:" + Rels.KNOWS + "]->(f1),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(f3),\n"
                   + "(person0)-[:" + Rels.KNOWS + "]->(f4),\n"
                   + "(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   /*
                    * Person-Comment
                    */
                   + "(f1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c111111),\n"
                   + "(f1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c1111),\n"
                   + "(f1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c11211),\n"
                   + "(f1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c121111),\n"
                   + "(f2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c1112),\n"
                   + "(f2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c112111),\n"
                   + "(f3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c112112),\n"
                   + "(f3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c111),\n"
                   + "(f3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c211),\n"
                   + "(f3)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c2111),\n"
                   + "(ff11)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c11112),\n"
                   + "(ff11)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c11111),\n"
                   + "(ff11)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(c1121),\n"
                   /*
                    * Comment-Comment
                    */
                   + "(c1111)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c11111),\n"
                   + "(c1111)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c11112),\n"
                   + "(c11111)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c111111),\n"
                   + "(c1121)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c11211),\n"
                   + "(c11211)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c112111),\n"
                   + "(c11211)<-[:" + Rels.REPLY_OF_COMMENT + "]-(c112112),\n"
                   /*
                    * Comment-Post
                    */
                   + "(p11)<-[:" + Rels.REPLY_OF_POST + "]-(c111),\n"
                   + "(p111)<-[:" + Rels.REPLY_OF_POST + "]-(c1111),\n"
                   + "(p111)<-[:" + Rels.REPLY_OF_POST + "]-(c1112),\n"
                   + "(p112)<-[:" + Rels.REPLY_OF_POST + "]-(c1121),\n"
                   + "(p12111)<-[:" + Rels.REPLY_OF_POST + "]-(c121111),\n"
                   + "(p21)<-[:" + Rels.REPLY_OF_POST + "]-(c211),\n"
                   + "(p211)<-[:" + Rels.REPLY_OF_POST + "]-(c2111),\n"
                   /*
                    * Post-Tag
                    */
                   + "(p11)-[:" + Rels.POST_HAS_TAG + "]->(t11),\n"
                   + "(p11)-[:" + Rels.POST_HAS_TAG + "]->(t12111),\n"
                   + "(p111)-[:" + Rels.POST_HAS_TAG + "]->(t111),\n"
                   + "(p112)-[:" + Rels.POST_HAS_TAG + "]->(t112),\n"
                   + "(p12111)-[:" + Rels.POST_HAS_TAG + "]->(t12111),\n"
                   + "(p12111)-[:" + Rels.POST_HAS_TAG + "]->(t21),\n"
                   + "(p21)-[:" + Rels.POST_HAS_TAG + "]->(t21),\n"
                   + "(p211)-[:" + Rels.POST_HAS_TAG + "]->(t211),\n"
                   /*
                    * Comment-Tag
                    */
                   + "(c1111)-[:" + Rels.COMMENT_HAS_TAG + "]->(t112),\n"
                   + "(c1121)-[:" + Rels.COMMENT_HAS_TAG + "]->(t11),\n"
                   + "(c11211)-[:" + Rels.COMMENT_HAS_TAG + "]->(t12111),\n"
                   /*
                    * Tag-TagClass
                    */
                   + "(tc1)<-[:" + Rels.HAS_TYPE + "]-(t11),\n"
                   + "(tc11)<-[:" + Rels.HAS_TYPE + "]-(t111),\n"
                   + "(tc11)<-[:" + Rels.HAS_TYPE + "]-(t112),\n"
                   + "(tc1211)<-[:" + Rels.HAS_TYPE + "]-(t12111),\n"
                   + "(tc2)<-[:" + Rels.HAS_TYPE + "]-(t21),\n"
                   + "(tc21)<-[:" + Rels.HAS_TYPE + "]-(t211),\n"
                   /*
                    * TagClass-TagClass
                    */
                   + "(tc11)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc1),\n"
                   + "(tc12)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc1),\n"
                   + "(tc121)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc12),\n"
                   + "(tc1211)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc121),\n"
                   + "(tc21)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc2)\n";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "ff11", TestPersons.ff11(),
                    // Tags
                    "t11", TestTags.t11(),
                    "t111", TestTags.t111(),
                    "t112", TestTags.t112(),
                    "t12111", TestTags.t12111(),
                    "t21", TestTags.t21(),
                    "t211", TestTags.t211(),
                    // TagClasses
                    "tc1", TestTagClasses.tc1(),
                    "tc11", TestTagClasses.tc11(),
                    "tc12", TestTagClasses.tc12(),
                    "tc121", TestTagClasses.tc121(),
                    "tc1211", TestTagClasses.tc1211(),
                    "tc2", TestTagClasses.tc2(),
                    "tc21", TestTagClasses.tc21()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person" );
                params.put( Person.LAST_NAME, "0" );
                return params;
            }

            protected static Map<String,Object> f1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "f" );
                params.put( Person.LAST_NAME, "1" );
                return params;
            }

            protected static Map<String,Object> f2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "f" );
                params.put( Person.LAST_NAME, "2" );
                return params;
            }

            protected static Map<String,Object> f3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "f" );
                params.put( Person.LAST_NAME, "3" );
                return params;
            }

            protected static Map<String,Object> f4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "f" );
                params.put( Person.LAST_NAME, "4" );
                return params;
            }

            protected static Map<String,Object> ff11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 11L );
                params.put( Person.FIRST_NAME, "ff" );
                params.put( Person.LAST_NAME, "11" );
                return params;
            }
        }

        protected static class TestTags
        {
            protected static Map<String,Object> t11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag11-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> t111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag111-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> t112()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag112-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> t12111()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag12111-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> t21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag21-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }

            protected static Map<String,Object> t211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Tag.NAME, "tag211-\u16a0\u3055\u4e35\u05e4\u0634" );
                return params;
            }
        }

        protected static class TestTagClasses
        {
            protected static Map<String,Object> tc1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "1" );
                return params;
            }

            protected static Map<String,Object> tc11()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "11" );
                return params;
            }

            protected static Map<String,Object> tc12()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "12" );
                return params;
            }

            protected static Map<String,Object> tc121()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "121" );
                return params;
            }

            protected static Map<String,Object> tc1211()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "1211" );
                return params;
            }

            protected static Map<String,Object> tc2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "2" );
                return params;
            }

            protected static Map<String,Object> tc21()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( TagClass.NAME, "21" );
                return params;
            }
        }
    }

    public static class Query13GraphMaker extends QueryGraphMaker
    {

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (p0:" + Nodes.Person + " {" + Person.ID + ":0}),\n"
                   + " (p1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + " (p2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + " (p3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + " (p4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + " (p5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                   + " (p6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                   + " (p7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"
                   + " (p8:" + Nodes.Person + " {" + Person.ID + ":8}),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(p0)-[:" + Rels.KNOWS + "]->(p1),\n"
                   + "(p1)-[:" + Rels.KNOWS + "]->(p3),\n"
                   + "(p1)<-[:" + Rels.KNOWS + "]-(p2),\n"
                   + "(p3)-[:" + Rels.KNOWS + "]->(p2),\n"
                   + "(p2)<-[:" + Rels.KNOWS + "]-(p4),\n"
                   + "(p4)-[:" + Rels.KNOWS + "]->(p7),\n"
                   + "(p4)-[:" + Rels.KNOWS + "]->(p6),\n"
                   + "(p6)<-[:" + Rels.KNOWS + "]-(p5)";
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map();
        }
    }

    public static class Query14GraphMaker extends QueryGraphMaker
    {

        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Countries
                    */
                   + "(country0:" + Place.Type.Country + " {" + Place.ID + ":0}),\n"
                   /*
                    * Persons
                    */
                   + "(p0:" + Nodes.Person + " {" + Person.ID + ":0}),\n"
                   + "(p1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                   + "(p2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                   + "(p3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                   + "(p4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                   + "(p5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                   + "(p6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                   + "(p7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"
                   + "(p8:" + Nodes.Person + " {" + Person.ID + ":8}),\n"
                   + "(p9:" + Nodes.Person + " {" + Person.ID + ":9}),\n"
                   /*
                    * Posts
                    */
                   + "(p0Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":0," + Message.CREATION_DATE + ":1}),\n"
                   + "(p1Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":1," + Message.CREATION_DATE + ":1}),\n"
                   + "(p3Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":2," + Message.CREATION_DATE + ":1}),\n"
                   + "(p5Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":3," + Message.CREATION_DATE + ":1}),\n"
                   + "(p6Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":4," + Message.CREATION_DATE + ":1}),\n"
                   + "(p7Post1:" + Nodes.Post + ":" + Nodes.Message
                   + " {" + Message.ID + ":5," + Message.CREATION_DATE + ":1}),\n"
                   /*
                    * Comments
                    */
                   + "(p0Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":6," + Message.CREATION_DATE + ":1}),\n"
                   + "(p1Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":7," + Message.CREATION_DATE + ":1}),\n"
                   + "(p1Comment2:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":8," + Message.CREATION_DATE + ":1}),\n"
                   + "(p4Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":9," + Message.CREATION_DATE + ":1}),\n"
                   + "(p4Comment2:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":10," + Message.CREATION_DATE + ":1}),\n"
                   + "(p5Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":11," + Message.CREATION_DATE + ":1}),\n"
                   + "(p5Comment2:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":12," + Message.CREATION_DATE + ":1}),\n"
                   + "(p7Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":13," + Message.CREATION_DATE + ":1}),\n"
                   + "(p8Comment1:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":14," + Message.CREATION_DATE + ":1}),\n"
                   + "(p8Comment2:" + Nodes.Comment + ":" + Nodes.Message
                   + " {" + Message.ID + ":15," + Message.CREATION_DATE + ":1}),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(p0)-[:" + Rels.KNOWS + "]->(p1),\n"
                   + "(p1)-[:" + Rels.KNOWS + "]->(p3),\n"
                   + "(p1)<-[:" + Rels.KNOWS + "]-(p2),\n"
                   + "(p1)<-[:" + Rels.KNOWS + "]-(p7),\n"
                   + "(p3)-[:" + Rels.KNOWS + "]->(p2),\n"
                   + "(p2)<-[:" + Rels.KNOWS + "]-(p4),\n"
                   + "(p4)-[:" + Rels.KNOWS + "]->(p7),\n"
                   + "(p4)-[:" + Rels.KNOWS + "]->(p8),\n"
                   + "(p4)-[:" + Rels.KNOWS + "]->(p6),\n"
                   + "(p6)<-[:" + Rels.KNOWS + "]-(p5),\n"
                   + "(p8)<-[:" + Rels.KNOWS + "]-(p5),\n"
                   /*
                    * Person-Post
                    */
                   + "(p0)<-[:" + Rels.POST_HAS_CREATOR + "]-(p0Post1),\n"
                   + "(p1)<-[:" + Rels.POST_HAS_CREATOR + "]-(p1Post1),\n"
                   + "(p3)<-[:" + Rels.POST_HAS_CREATOR + "]-(p3Post1),\n"
                   + "(p5)<-[:" + Rels.POST_HAS_CREATOR + "]-(p5Post1),\n"
                   + "(p6)<-[:" + Rels.POST_HAS_CREATOR + "]-(p6Post1),\n"
                   + "(p7)<-[:" + Rels.POST_HAS_CREATOR + "]-(p7Post1),\n"
                   /*
                    * Person-Comment
                    */
                   + "(p0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p0Comment1),\n"
                   + "(p1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p1Comment1),\n"
                   + "(p1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p1Comment2),\n"
                   + "(p4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p4Comment1),\n"
                   + "(p4)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p4Comment2),\n"
                   + "(p5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p5Comment1),\n"
                   + "(p5)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p5Comment2),\n"
                   + "(p7)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p7Comment1),\n"
                   + "(p8)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p8Comment1),\n"
                   + "(p8)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(p8Comment2),\n"
                   /*
                    * Comment-Post
                    */
                   + "(p1Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p0Comment1),\n"
                   + "(p0Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p1Comment1),\n"
                   + "(p0Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p1Comment2),\n"
                   + "(p3Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p4Comment1),\n"
                   + "(p7Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p4Comment2),\n"
                   + "(p5Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p5Comment1),\n"
                   + "(p6Post1)<-[:" + Rels.REPLY_OF_POST + "]-(p8Comment1),\n"
                   /*
                    * Comment-Comment
                    */
                   + "(p7Comment1)-[:" + Rels.REPLY_OF_COMMENT + "]->(p4Comment2),\n"
                   + "(p8Comment2)-[:" + Rels.REPLY_OF_COMMENT + "]->(p4Comment1),\n"
                   + "(p5Comment2)-[:" + Rels.REPLY_OF_COMMENT + "]->(p8Comment2)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map( "country0", TestCountries.country0() );
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 0L );
                params.put( Place.NAME, "country0" );
                return params;
            }
        }
    }

    public static class ShortQuery1GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   + " (person2:" + Nodes.Person + " $person2),\n"
                   /*
                    * Cities
                    */
                   + " (city0:" + Place.Type.City + " $city0),\n"
                   + " (city1:" + Place.Type.City + " $city1),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-City
                    */
                   + "(person0)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city0),\n"
                   + "(person1)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1),\n"
                   + "(person2)-[:" + Rels.PERSON_IS_LOCATED_IN + "]->(city1)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    "person2", TestPersons.person2(),
                    // Cities
                    "city0", TestCities.city0(),
                    "city1", TestCities.city1()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> person2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "person2" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.LOCATION_IP, "ip2" );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.CREATION_DATE, 2L );
                return params;
            }
        }

        protected static class TestCities
        {
            protected static Map<String,Object> city0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 0L );
                params.put( Place.NAME, "city0" );
                return params;
            }

            protected static Map<String,Object> city1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 1L );
                params.put( Place.NAME, "city1" );
                return params;
            }
        }
    }

    public static class ShortQuery2GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   + " (post2:" + Nodes.Post + ":" + Nodes.Message + " $post2),\n"
                   /*
                    * Comments
                    */
                   + " (comment3:" + Nodes.Comment + ":" + Nodes.Message + " $comment3),\n"
                   + " (comment4:" + Nodes.Comment + ":" + Nodes.Message + " $comment4),\n"
                   + " (comment5:" + Nodes.Comment + ":" + Nodes.Message + " $comment5),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Post
                    */
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post2),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   + "(person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment3),\n"
                   + "(person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment4),\n"
                   /*
                    * Post-Comment-Comment
                    */
                   + "(post0)<-[:" + Rels.REPLY_OF_POST + "]-(comment3)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment5),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment4)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    "post2", TestPosts.post2(),
                    // Posts
                    "comment3", TestComments.comment3(),
                    "comment4", TestComments.comment4(),
                    "comment5", TestComments.comment5()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CREATION_DATE, 0L );
                params.put( Message.CONTENT, "post_content0" );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Post.IMAGE_FILE, "post_image1" );
                return params;
            }

            protected static Map<String,Object> post2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Post.IMAGE_FILE, "post_image2" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.CONTENT, "comment_content3" );
                return params;
            }

            protected static Map<String,Object> comment4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.CONTENT, "comment_content4" );
                return params;
            }

            protected static Map<String,Object> comment5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CREATION_DATE, 5L );
                params.put( Message.CONTENT, "comment_content5" );
                return params;
            }
        }
    }

    public static class ShortQuery3GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   + " (person2:" + Nodes.Person + " $person2),\n"
                   + " (person3:" + Nodes.Person + " $person3),\n"
                   + " (person4:" + Nodes.Person + " $person4),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person0)-[:" + Rels.KNOWS + "{" + Knows.CREATION_DATE + ":" + 0 + "}]->(person1),\n"

                   + "(person0)-[:" + Rels.KNOWS + "{" + Knows.CREATION_DATE + ":" + 0 + "}]->(person2),\n"

                   + "(person0)-[:" + Rels.KNOWS + "{" + Knows.CREATION_DATE + ":" + 2 + "}]->(person3),\n"

                   + "(person1)-[:" + Rels.KNOWS + "{" + Knows.CREATION_DATE + ":" + 3 + "}]->(person2)\n"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    "person2", TestPersons.person2(),
                    "person3", TestPersons.person3(),
                    "person4", TestPersons.person4()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> person2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "person2" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.LOCATION_IP, "ip2" );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.CREATION_DATE, 2L );
                return params;
            }

            protected static Map<String,Object> person3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 3L );
                params.put( Person.FIRST_NAME, "person3" );
                params.put( Person.LAST_NAME, "three-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 3L );
                params.put( Person.LOCATION_IP, "ip3" );
                params.put( Person.BROWSER_USED, "browser3" );
                params.put( Person.GENDER, "gender3" );
                params.put( Person.CREATION_DATE, 3L );
                return params;
            }

            protected static Map<String,Object> person4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 4L );
                params.put( Person.FIRST_NAME, "person4" );
                params.put( Person.LAST_NAME, "four-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 4L );
                params.put( Person.LOCATION_IP, "ip4" );
                params.put( Person.BROWSER_USED, "browser4" );
                params.put( Person.GENDER, "gender4" );
                params.put( Person.CREATION_DATE, 4L );
                return params;
            }
        }
    }

    public static class ShortQuery4GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   /*
                    * Comments
                    */
                   + " (comment0:" + Nodes.Comment + ":" + Nodes.Message + " $comment0),\n"
                   + " (comment1:" + Nodes.Comment + ":" + Nodes.Message + " $comment1)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    // Posts
                    "comment0", TestComments.comment0(),
                    "comment1", TestComments.comment1()
            );
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CREATION_DATE, 0L );
                params.put( Message.CONTENT, "post_content0" );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Post.IMAGE_FILE, "post_image1" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.CONTENT, "comment_content2" );
                return params;
            }

            protected static Map<String,Object> comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.CONTENT, "comment_content3" );
                return params;
            }
        }
    }

    public static class ShortQuery5GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   /*
                    * Comments
                    */
                   + " (comment0:" + Nodes.Comment + ":" + Nodes.Message + " $comment0),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Post
                    */
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment0),\n"
                   /*
                    * Post-Comment
                    */
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment0)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    // Posts
                    "comment0", TestComments.comment0()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CREATION_DATE, 0L );
                params.put( Message.CONTENT, "post_content0" );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Post.IMAGE_FILE, "post_image1" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.CONTENT, "comment_content2" );
                return params;
            }
        }
    }

    public static class ShortQuery6GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   /*
                    * Forums
                    */
                   + " (forum0:" + Nodes.Forum + " $forum0),\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   /*
                    * Comments
                    */
                   + " (comment0:" + Nodes.Comment + ":" + Nodes.Message + " $comment0),\n"
                   + " (comment1:" + Nodes.Comment + ":" + Nodes.Message + " $comment1),\n"
                   + " (comment2:" + Nodes.Comment + ":" + Nodes.Message + " $comment2),\n"
                   + " (comment3:" + Nodes.Comment + ":" + Nodes.Message + " $comment3),\n"
                   + " (comment4:" + Nodes.Comment + ":" + Nodes.Message + " $comment4),\n"
                   /*
                    * Countries
                    */
                   + " (country0:" + Place.Type.Country + " $country0),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-HAS_MODERATOR-Forum
                    */
                   + "(person0)<-[:" + Rels.HAS_MODERATOR + "]-(forum0),\n"
                   /*
                    * Person-HAS_MEMBER-Forum
                    */
                   + "(person0)<-[:" + Rels.HAS_MEMBER + " {" + HasMember.JOIN_DATE + ":1}]-(forum0),\n"
                   + "(person1)<-[:" + Rels.HAS_MEMBER + " {" + HasMember.JOIN_DATE + ":1}]-(forum0),\n"
                   /*
                    * Forum-Post
                    */
                   + "(forum0)-[:" + Rels.CONTAINER_OF + "]->(post0),\n"
                   + "(forum0)-[:" + Rels.CONTAINER_OF + "]->(post1),\n"
                   /*
                    * Person-Post
                    */
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment0),\n"
                   + "(person1)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment1),\n"
                   /*
                    * Post-Comment
                    */
                   + "(post0)<-[:" + Rels.REPLY_OF_POST + "]-(comment0)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment1)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment2)<-[:" + Rels.REPLY_OF_COMMENT +
                   "]-(comment3),\n"
                   + "(comment2)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment4)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    // Forums
                    "forum0", TestForums.forum0(),
                    // Persons
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    // Countries
                    "country0", TestCountries.country0(),
                    // Posts
                    "comment0", TestComments.comment0(),
                    "comment1", TestComments.comment1(),
                    "comment2", TestComments.comment2(),
                    "comment3", TestComments.comment3(),
                    "comment4", TestComments.comment4()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }
        }

        protected static class TestForums
        {
            protected static Map<String,Object> forum0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Forum.ID, 0L );
                params.put( Forum.TITLE, "forum0" );
                params.put( Forum.CREATION_DATE, 0L );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CREATION_DATE, 0L );
                params.put( Message.CONTENT, "post_content0" );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.CONTENT, "post_content1" );
                return params;
            }
        }

        protected static class TestCountries
        {
            protected static Map<String,Object> country0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Place.ID, 0L );
                params.put( Place.NAME, "place0" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 10L );
                params.put( Message.CREATION_DATE, 10L );
                params.put( Message.CONTENT, "comment_content0" );
                return params;
            }

            protected static Map<String,Object> comment1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 11L );
                params.put( Message.CREATION_DATE, 11L );
                params.put( Message.CONTENT, "comment_content1" );
                return params;
            }

            protected static Map<String,Object> comment2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 12L );
                params.put( Message.CREATION_DATE, 12L );
                params.put( Message.CONTENT, "comment_content2" );
                return params;
            }

            protected static Map<String,Object> comment3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 13L );
                params.put( Message.CREATION_DATE, 13L );
                params.put( Message.CONTENT, "comment_content3" );
                return params;
            }

            protected static Map<String,Object> comment4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 14L );
                params.put( Message.CREATION_DATE, 14L );
                params.put( Message.CONTENT, "comment_content4" );
                return params;
            }
        }
    }

    public static class ShortQuery7GraphMaker extends QueryGraphMaker
    {
        @Override
        public String queryString()
        {
            return "CREATE\n"
                   + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                   + " (person0:" + Nodes.Person + " $person0),\n"
                   + " (person1:" + Nodes.Person + " $person1),\n"
                   + " (person2:" + Nodes.Person + " $person2),\n"
                   /*
                    * Posts
                    */
                   + " (post0:" + Nodes.Post + ":" + Nodes.Message + " $post0),\n"
                   + " (post1:" + Nodes.Post + ":" + Nodes.Message + " $post1),\n"
                   /*
                    * Comments
                    */
                   + " (comment2:" + Nodes.Comment + ":" + Nodes.Message + " $comment2),\n"
                   + " (comment3:" + Nodes.Comment + ":" + Nodes.Message + " $comment3),\n"
                   + " (comment4:" + Nodes.Comment + ":" + Nodes.Message + " $comment4),\n"
                   + " (comment5:" + Nodes.Comment + ":" + Nodes.Message + " $comment5),\n"

                   + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                   + "(person1)<-[:" + Rels.KNOWS + "]-(person2),\n"
                   /*
                    * Person-Post
                    */
                   + "(person0)<-[:" + Rels.POST_HAS_CREATOR + "]-(post0),\n"
                   + "(person1)<-[:" + Rels.POST_HAS_CREATOR + "]-(post1),\n"
                   /*
                    * Person-Comment
                    */
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment2),\n"
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment3),\n"
                   + "(person0)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment4),\n"
                   + "(person2)<-[:" + Rels.COMMENT_HAS_CREATOR + "]-(comment5),\n"
                   /*
                    * Post-Comment
                    */
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment2),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment3),\n"
                   + "(post1)<-[:" + Rels.REPLY_OF_POST + "]-(comment5),\n"
                   /*
                    * Comment-Comment
                    */
                   + "(comment2)<-[:" + Rels.REPLY_OF_COMMENT + "]-(comment4)"
                    ;
        }

        @Override
        public Map<String,Object> params()
        {
            return MapUtil.map(
                    "person0", TestPersons.person0(),
                    "person1", TestPersons.person1(),
                    "person2", TestPersons.person2(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    // Posts
                    "comment2", TestComments.comment2(),
                    "comment3", TestComments.comment3(),
                    "comment4", TestComments.comment4(),
                    "comment5", TestComments.comment5()
            );
        }

        protected static class TestPersons
        {
            protected static Map<String,Object> person0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 0L );
                params.put( Person.FIRST_NAME, "person0" );
                params.put( Person.LAST_NAME, "zero-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 0L );
                params.put( Person.LOCATION_IP, "ip0" );
                params.put( Person.BROWSER_USED, "browser0" );
                params.put( Person.GENDER, "gender0" );
                params.put( Person.CREATION_DATE, 0L );
                return params;
            }

            protected static Map<String,Object> person1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 1L );
                params.put( Person.FIRST_NAME, "person1" );
                params.put( Person.LAST_NAME, "one-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 1L );
                params.put( Person.LOCATION_IP, "ip1" );
                params.put( Person.BROWSER_USED, "browser1" );
                params.put( Person.GENDER, "gender1" );
                params.put( Person.CREATION_DATE, 1L );
                return params;
            }

            protected static Map<String,Object> person2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Person.ID, 2L );
                params.put( Person.FIRST_NAME, "person2" );
                params.put( Person.LAST_NAME, "two-\u16a0\u3055\u4e35\u05e4\u0634" );
                params.put( Person.BIRTHDAY, 2L );
                params.put( Person.LOCATION_IP, "ip2" );
                params.put( Person.BROWSER_USED, "browser2" );
                params.put( Person.GENDER, "gender2" );
                params.put( Person.CREATION_DATE, 2L );
                return params;
            }
        }

        protected static class TestPosts
        {
            protected static Map<String,Object> post0()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 0L );
                params.put( Message.CREATION_DATE, 0L );
                params.put( Message.CONTENT, "post_content0" );
                return params;
            }

            protected static Map<String,Object> post1()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 1L );
                params.put( Message.CREATION_DATE, 1L );
                params.put( Message.CONTENT, "post_image1" );
                return params;
            }
        }

        protected static class TestComments
        {
            protected static Map<String,Object> comment2()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 2L );
                params.put( Message.CREATION_DATE, 2L );
                params.put( Message.CONTENT, "comment_content2" );
                return params;
            }

            protected static Map<String,Object> comment3()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 3L );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.CONTENT, "comment_content3" );
                return params;
            }

            protected static Map<String,Object> comment4()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 4L );
                params.put( Message.CREATION_DATE, 4L );
                params.put( Message.CONTENT, "comment_content4" );
                return params;
            }

            protected static Map<String,Object> comment5()
            {
                Map<String,Object> params = new HashMap<>();
                params.put( Message.ID, 5L );
                params.put( Message.CREATION_DATE, 3L );
                params.put( Message.CONTENT, "comment_content5" );
                return params;
            }
        }
    }
}
