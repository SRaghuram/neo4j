/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static cypher.SpecSuiteConstants.BLACKLIST_PLUGIN;
import static cypher.SpecSuiteConstants.CYPHER_OPTION_PLUGIN;
import static cypher.SpecSuiteConstants.DB_CONFIG;
import static cypher.SpecSuiteConstants.GLUE_PATH;
import static cypher.SpecSuiteConstants.HTML_REPORT;
import static cypher.SpecSuiteConstants.JSON_REPORT;

@RunWith( Enclosed.class )
public class AcceptanceSpecSuiteTest
{

    // These two constants are only used to make testing and debugging easier.
    // If you want to run only a single feature, put the name of the feature file in `FEATURE_TO_RUN` (including .feature)
    // If you want to run only a single scenario, put (part of) its name in the `SCENARIO_NAME_REQUIRED` constant
    // Do not forget to clear these strings to empty strings before you commit!!
    public static final String FEATURE_TO_RUN = "";
    public static final String SCENARIO_NAME_REQUIRED = "";

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost.json",
                    HTML_REPORT + SUITE_NAME + "/cost",
                    JSON_REPORT + SUITE_NAME + "/cost",
                    BLACKLIST_PLUGIN + "cost.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Cost
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost-compiled.json",
                    HTML_REPORT + SUITE_NAME + "/cost-compiled",
                    JSON_REPORT + SUITE_NAME + "/cost-compiled",
                    BLACKLIST_PLUGIN + "cost-compiled.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class CostCompiled
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "cost-compiled.json",
                    HTML_REPORT + SUITE_NAME + "/cost-compiled-sourcecode",
                    JSON_REPORT + SUITE_NAME + "/cost-compiled-sourcecode",
                    BLACKLIST_PLUGIN + "cost-compiled.txt",
                    CYPHER_OPTION_PLUGIN + "cost-compiled-sourcecode.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class CostCompiledSourceCode
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-23.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-23",
                    JSON_REPORT + SUITE_NAME + "/compatibility-23",
                    BLACKLIST_PLUGIN + "compatibility-23.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility23
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    DB_CONFIG + "compatibility-31.json",
                    HTML_REPORT + SUITE_NAME + "/compatibility-31",
                    JSON_REPORT + SUITE_NAME + "/compatibility-31",
                    BLACKLIST_PLUGIN + "compatibility-31.txt"
            },
            glue = { GLUE_PATH },
            features = { FEATURE_PATH + FEATURE_TO_RUN },
            tags = { "~@pending" },
            strict = true
    )
    public static class Compatibility31
    {
    }

    // constants for TCK configuration

    public static final String SUITE_NAME = "acceptance-spec-suite";

    @SuppressWarnings( "unused" )
    public static final Class<?> RESOURCE_CLASS = AcceptanceSpecSuiteTest.class;

    private static final String FEATURE_PATH = "src/test/resources/cypher/features/";
}
