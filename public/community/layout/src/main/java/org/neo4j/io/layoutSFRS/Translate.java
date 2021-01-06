package org.neo4j.io.layoutSFRS;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

public class Translate {

    static public DatabaseLayout translate(org.neo4j.io.layoutSFRS.DatabaseLayout in)
    {
        Neo4jLayout storeLayout = translate(in.getNeo4jLayout());
        DatabaseLayout out = DatabaseLayout.of(storeLayout, in.getDatabaseName());
        return out;
    }

    static public org.neo4j.io.layoutSFRS.DatabaseLayout translate(DatabaseLayout in)
    {
        org.neo4j.io.layoutSFRS.Neo4jLayout storeLayout = translate(in.getNeo4jLayout());
        org.neo4j.io.layoutSFRS.DatabaseLayout out = org.neo4j.io.layoutSFRS.DatabaseLayout.of(storeLayout, in.getDatabaseName());
        return out;
    }

    static public org.neo4j.io.layoutSFRS.DatabaseLayout translate(DatabaseLayout in, boolean isSFRSEngine, boolean isOneIDFile)
    {
        org.neo4j.io.layoutSFRS.Neo4jLayout storeLayout = translate(in.getNeo4jLayout());
        org.neo4j.io.layoutSFRS.DatabaseLayout out = org.neo4j.io.layoutSFRS.DatabaseLayout.of(storeLayout, in.getDatabaseName());
        out.setSFRSEngine( isSFRSEngine );
        out.setIsOneIDFile( isOneIDFile );
        return out;
    }

    static public Neo4jLayout translate(org.neo4j.io.layoutSFRS.Neo4jLayout in)
    {
        return Neo4jLayout.of(in.homeDirectory());
    }

    static public org.neo4j.io.layoutSFRS.Neo4jLayout translate(Neo4jLayout in)
    {
        return org.neo4j.io.layoutSFRS.Neo4jLayout.of(in.homeDirectory());
    }


}

