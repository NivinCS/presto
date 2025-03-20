/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestHiveMixedCaseSupport extends ProductTest
{

    /*
     * Cleanup test case for removing schemas and tables created during mixed-case table tests.
     *
     * This test ensures that all test schemas and tables are properly dropped to maintain a clean test environment.
     */
    @Test
    public void testCleanupMixedCaseTablesAndSchemas() {
        String schemaName = "hivemixedcaseon";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        // Drop tables if they exist
        query("DROP TABLE IF EXISTS " + schemaName + ".testtable0");
        query("DROP TABLE IF EXISTS " + schemaName + ".testtable");
        query("DROP TABLE IF EXISTS " + schemaName + ".TestTable1");
        query("DROP TABLE IF EXISTS " + schemaName + ".TESTTABLE2");
        query("DROP TABLE IF EXISTS " + schemaNameUpper + ".TESTTABLE4");
        query("DROP TABLE IF EXISTS " + schemaNameUpper + ".testtable02");
        query("DROP TABLE IF EXISTS " + schemaNameUpper + ".TESTTABLE3");

        // Drop schemas if they exist
        query("DROP SCHEMA IF EXISTS " + schemaName);
        query("DROP SCHEMA IF EXISTS " + schemaNameUpper);
    }

    /*
     * Test cases for creating schemas with different naming conventions in Hive.
     *
     * This class includes tests for:
     * 1. Creating a schema with a lowercase name.
     * 2. Creating a schema with a mixed-case name that has the same syllables as an existing schema.
     * 3. Creating a schema with a mixed-case name.
     * 4. Creating a schema with an uppercase name.
     *
     * Each test ensures the schema is created successfully.
     */
    @Test
    public void testCreateSchemasWithMixedCaseNames()
    {
        // Define schema names and their expected stored values in Hive
        String schemaNameLower = "hivemixedcaseon";
        String schemaNameMixedSyllables = "HiveMixedCaseOn";
        String schemaNameMixed = "HiveMixedCase";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        String schemaNameLowerStored = "hivemixedcaseon";
        String schemaNameMixedSyllablesStored = "hivemixedcaseon";
        String schemaNameMixedStored = "hivemixedcase";
        String schemaNameUpperStored = "hivemixedcaseon1";

        // Create schemas
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameLower);
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameMixedSyllables);
        query("CREATE SCHEMA IF NOT EXISTS " + schemaNameMixed);
        query("CREATE SCHEMA " + schemaNameUpper);

        // Verify schema creation
        assertThat(query("SHOW SCHEMAS"))
                .contains(row(schemaNameLowerStored))
                .contains(row(schemaNameMixedSyllablesStored))
                .contains(row(schemaNameMixedStored))
                .contains(row(schemaNameUpperStored));
    }

    /*
     * Test cases for creating tables with different naming conventions in Hive.
     *
     * This test verifies table creation, schema behavior, and column definitions
     * when using different case variations for table and column names.
     *
     * Scenarios covered:
     * 1. Creating tables with lowercase, mixed-case, and uppercase names.
     * 2. Creating tables with uppercase column names.
     * 3. Creating tables in schemas with lowercase and uppercase names.
     *
     * The test ensures:
     * - Tables are created successfully.
     * - Table names are stored and retrieved correctly.
     * - Column definitions match expected types.
     */
    @Test
    public void testCreateTablesWithMixedCaseNames() {
        String schemaName = "hivemixedcaseon";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        // Creating tables with different name conventions
        query("CREATE TABLE " + schemaName + ".testtable0 (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + schemaName + ".testtable (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + schemaName + ".TestTable1 (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + schemaName + ".TESTTABLE2 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + schemaNameUpper + ".TESTTABLE4 (Name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + schemaNameUpper + ".testtable02 (name VARCHAR(50), id INT, num DOUBLE)");
        query("CREATE TABLE " + schemaNameUpper + ".TESTTABLE3 (name VARCHAR(50), id INT, num DOUBLE)");

        // Verify table existence and column properties
        assertThat(query("SHOW TABLES FROM " + schemaName))
                .containsOnly(row("testtable0"), row("testtable"), row("testtable1"), row("testtable2"));

        assertThat(query("SHOW TABLES FROM " + schemaNameUpper))
                .containsOnly(row("testtable4"), row("testtable02"), row("testtable3"));

        assertThat(query("DESCRIBE " + schemaName + ".testtable0"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + schemaName + ".testtable"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + schemaNameUpper + ".testtable4"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""));

        assertThat(query("DESCRIBE " + schemaNameUpper + ".testtable02"))
                .contains(row("name", "varchar(50)", "", ""), row("id", "integer", "", ""), row("num", "double", "", ""));


    }

    /*
     * This test validates inserting data into tables with different case variations in schema and table names.
     * It ensures that data is inserted and retrieved correctly regardless of case sensitivity.
     */
    @Test
    public void testInsertDataWithMixedCaseNames() {
        String schemaName = "hivemixedcaseon";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        // Insert data into tables with different case variations
        query("INSERT INTO " + schemaName + ".testtable VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + schemaName + ".TESTTABLE2 VALUES ('ann', 112), ('mary', 123)");
        query("INSERT INTO " + schemaNameUpper + ".testtable02 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");
        query("INSERT INTO " + schemaNameUpper + ".TESTTABLE3 VALUES ('emma', 112, 200.002), ('mark', 123, 300.003)");

        // Validate inserted data
        assertThat(query("SELECT * FROM " + schemaName + ".testtable"))
                .containsOnly(row("amy", 112), row("mia", 123));
        assertThat(query("SELECT * FROM " + schemaName + ".TESTTABLE2"))
                .containsOnly(row("ann", 112), row("mary", 123));
        assertThat(query("SELECT * FROM " + schemaNameUpper + ".testtable02"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
        assertThat(query("SELECT * FROM " + schemaNameUpper + ".TESTTABLE3"))
                .containsOnly(row("emma", 112, 200.002), row("mark", 123, 300.003));
    }

    /*
     * This test verifies selecting data from tables with different case variations in schema and table names.
     * It ensures that queries return correct results regardless of case sensitivity.
     */
    @Test
    public void testSelectDataWithMixedCaseNames() {
        String schemaName = "hivemixedcaseon";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        // Select all data from a lowercase table name
        assertThat(query("SELECT * FROM " + schemaName + ".testtable"))
                .containsOnly(row("amy", 112), row("mia", 123));

        // Select the data within a condition from a mixed-case table name, lowercase schema name
        assertThat(query("SELECT name FROM " + schemaName + ".TESTTABLE2 WHERE id = 123"))
                .containsOnly(row("mary"));

        // Select the data within a condition from a lowercase table name, uppercase schema name
        assertThat(query("SELECT name FROM " + schemaNameUpper + ".testtable02 WHERE id = 112"))
                .containsOnly(row("emma"));

        // Select data within a condition from a mixed-case table name in a mixed-case schema name
        assertThat(query("SELECT name FROM " + schemaNameUpper + ".TESTTABLE3 WHERE id = 123"))
                .containsOnly(row("mark"));
    }

    /*
     * This test verifies altering tables with different case variations in schema and table names.
     * It ensures that columns can be added renamed irrespective of case sensitivity.
     */
    @Test
    public void testTableAlterWithMixedCaseNames() {
        String schemaName = "hivemixedcaseon";
        String schemaNameUpper = "HIVEMIXEDCASEON1";

        // Add columns to tables with various schema and table name cases
        query("ALTER TABLE " + schemaName + ".testtable ADD COLUMN num REAL");
        query("ALTER TABLE " + schemaNameUpper + ".testtable02 ADD COLUMN num1 REAL");
        query("ALTER TABLE " + schemaName + ".TESTTABLE2 ADD COLUMN num01 REAL");
        query("ALTER TABLE " + schemaNameUpper + ".TESTTABLE02 ADD COLUMN num2 REAL");

        // Verify the added columns
        assertThat(query("DESCRIBE hivemixedcaseon.testtable"))
                .contains(row("num", "real", "", ""));
        assertThat(query("DESCRIBE " + schemaNameUpper + ".testtable02"))
                .contains(row("num1", "real", "", ""));
        assertThat(query("DESCRIBE " + schemaName + ".TESTTABLE2"))
                .contains(row("num01", "real", "", ""));
        assertThat(query("DESCRIBE " + schemaNameUpper + ".TESTTABLE02"))
                .contains(row("num2", "real", "", ""));

        // Rename columns
        query("ALTER TABLE " + schemaName + ".testtable RENAME COLUMN num TO numb");
        query("ALTER TABLE " + schemaNameUpper + ".testtable02 RENAME COLUMN num1 TO numb01");

        // Verify column renaming
        assertThat(query("DESCRIBE " + schemaName + ".testtable"))
                .contains(row("numb", "real", "", ""));
        assertThat(query("DESCRIBE " + schemaNameUpper + ".testtable02"))
                .contains(row("numb01", "real", "", ""));

    }
}
