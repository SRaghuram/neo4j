@echo off

call mvn dependency:copy-dependencies

call java -cp "target\dependency\*;target\classes"  com.neo4j.tools.dump.DumpLogicalLog %*
