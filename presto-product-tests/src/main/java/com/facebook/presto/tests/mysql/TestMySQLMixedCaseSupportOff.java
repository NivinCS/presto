/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests.mysql;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestMySQLMixedCaseSupportOff
        extends ProductTest
{
    private static final String CATALOG = "mysql";
    /* MySQL connector does not support creating new schemas, so an existing schema is being used instead*/
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "testtable";
    private static final String TABLE_NAME_0 = "testtable0";
    private static final String TABLE_NAME_MIXED_1 = "TestTable1";
    private static final String TABLE_NAME_UPPER_2 = "TESTTABLE2";

    /*
     * Cleanup test case for removing tables created during mixed-case table tests.
     *
     * This test ensures that all test schemas and tables are properly dropped to maintain a clean test environment.
     */
    @Test
    public void testCleanupMixedCaseTablesAndSchemas()
    {
        query("DROP TABLE IF EXISTS " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME);
        query("DROP TABLE IF EXISTS " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_0);
        query("DROP TABLE IF EXISTS " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_MIXED_1);
        query("DROP TABLE IF EXISTS " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2);
    }

    /*
     * Test cases for creating tables with different naming conventions in MySQL.
     *
     * This test verifies table creation, schema behavior, and column definitions
     * when using different case variations for table and column names.
     */
    @Test
    public void testCreateTablesWithMixedCaseNames()
    {
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_0 + " (name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " (name VARCHAR(50), ID INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_MIXED_1 + " (Name VARCHAR(50), id INT)");
        query("CREATE TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " (Name VARCHAR(50), ID INT)");

        assertThat(query("SHOW TABLES FROM " + CATALOG + "." + SCHEMA_NAME))
                .containsOnly(row("testtable0"), row("testtable"), row("testtable1"), row("testtable2"));

    }

    /*
     * This test validates inserting data into tables with different case variations in table names.
     * It ensures that data is inserted and retrieved correctly regardless of case sensitivity.
     */
    @Test
    public void testInsertDataWithMixedCaseNames()
    {
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " VALUES ('amy', 112), ('mia', 123)");
        query("INSERT INTO " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " VALUES ('ann', 112), ('mary', 123)");

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .containsOnly(row("amy", 112), row("mia", 123));

        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2))
                .containsOnly(row("ann", 112), row("mary", 123));

    }

    /*
     * This test verifies selecting data from tables with different case variations in table names.
     * It ensures that queries return correct results regardless of case sensitivity.
     */
    @Test
    public void testSelectDataWithMixedCaseNames()
    {
        assertThat(query("SELECT * FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .containsOnly(row("amy", 112), row("mia", 123));

        assertThat(query("SELECT name FROM " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " WHERE id = 123"))
                .containsOnly(row("mary"));

    }

    /*
     * This test verifies altering tables with different case variations in table names.
     *
     * Scenarios covered:
     * 1. Adding columns to tables with lowercase, mixed-case, and uppercase names.
     * 2. Renaming columns in tables with various case patterns.
     *
     * The test ensures:
     * - Column addition works correctly across cases.
     * - Column renaming functions as expected.
     * - Case variations do not impact alter operations.
     */
    @Test
    public void testTableAlterWithMixedCaseNames()
    {
        // Add columns to tables with various schema and table name cases
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " ADD COLUMN num REAL");
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2 + " ADD COLUMN num01 REAL");

        // Verify the added columns
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .contains(row("num", "real", "", ""));
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME_UPPER_2))
                .contains(row("num01", "real", "", ""));

        // Rename columns
        query("ALTER TABLE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME + " RENAME COLUMN num TO numb");

        // Verify column renaming
        assertThat(query("DESCRIBE " + CATALOG + "." + SCHEMA_NAME + "." + TABLE_NAME))
                .contains(row("numb", "real", "", ""));
    }
}
