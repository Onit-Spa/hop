/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.databases.trino;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

@DatabaseMetaPlugin(
    type = "TRINO",
    typeDescription = "Trino",
    image = "trino.svg",
    documentationUrl = "/database/databases/trino.html")
@GuiPlugin(id = "GUI-TrinoDatabaseMeta")
public class TrinoDatabaseMeta extends BaseDatabaseMeta implements IDatabase {

  private IDatabase databaseDialect = null;

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {

    if (databaseDialect != null) {
      return databaseDialect.getFieldDefinition(v, tk, pk, useAutoIncrement, addFieldName, addCr);
    }

    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP:
      case IValueMeta.TYPE_DATE:
        retval += "TIMESTAMP";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          retval += "BIGSERIAL";
        } else {
          if (type == IValueMeta.TYPE_INTEGER) {
            // Integer values...
            if (length < 3) {
              retval += "TINYINT";
            } else if (length < 5) {
              retval += "SMALLINT";
            } else if (length < 10) {
              retval += "INT";
            } else if (length < 20) {
              retval += "BIGINT";
            } else {
              retval += "DECIMAL(" + length + ")";
            }
          } else if (type == IValueMeta.TYPE_BIGNUMBER) {
            // Fixed point value...
            if (length
                < 1) { // user configured no value for length. Use 16 digits, which is comparable to
              // mantissa 2^53 of IEEE 754 binary64 "double".
              length = 16;
            }
            if (precision
                < 1) { // user configured no value for precision. Use 16 digits, which is comparable
              // to IEEE 754 binary64 "double".
              precision = 16;
            }
            retval += "DECIMAL(" + length + "," + precision + ")";
          } else {
            // Floating point value with double precision...
            retval += "DOUBLE PRECISION";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length >= DatabaseMeta.CLOB_LENGTH) {
          retval += "TEXT";
        } else {
          retval += "VARCHAR";
          if (length > 0) {
            retval += "(" + length;
          } else {
            retval += "("; // Maybe use some default DB String length?
          }
          retval += ")";
        }
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if (addCr) {
      retval += Const.CR;
    }

    return retval;
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 8443;
    }
    return -1;
  }

  @Override
  public String getDriverClass() {
    return "io.trino.jdbc.TrinoDriver";
  }

  /**
   * @return The extra option separator in database URL for this platform (usually this is &)
   */
  @Override
  public String getExtraOptionSeparator() {
    return "&";
  }

  /**
   * @return This indicator separates the normal URL from the options (usually this is ?)
   */
  @Override
  public String getExtraOptionIndicator() {
    return "?";
  }

  /**
   * @return true if the database supports connection options in the URL, false if they are put in a
   *     Properties object. Trino requires options to be passed as Properties, not in the URL.
   */
  @Override
  public boolean isSupportsOptionsInURL() {
    return false;
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    StringBuilder url = new StringBuilder("jdbc:trino://");
    url.append(hostname);
    if (!Utils.isEmpty(port)) {
      url.append(":").append(port);
    }

    // Extract the actual database name from the databaseName parameter
    // In case it contains extra options (e.g., "catalog;option1=value1;option2=value2")
    String actualDatabaseName = databaseName;
    if (!Utils.isEmpty(databaseName) && databaseName.contains(";")) {
      actualDatabaseName = databaseName.substring(0, databaseName.indexOf(";"));
    }

    if (!Utils.isEmpty(actualDatabaseName)) {
      if (!actualDatabaseName.startsWith("/")) {
        url.append("/");
      }
      url.append(actualDatabaseName);
    }
    return url.toString();
  }

  /**
   * Generates the SQL statement to add a column to the specified table For this generic type, i set
   * it to the most common possibility.
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoIncrement whether this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether to add a semicolon behind the statement.
   * @return the SQL statement to add a column to the specified table
   */
  @Override
  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    if (databaseDialect != null) {
      return databaseDialect.getAddColumnStatement(
          tableName, v, tk, useAutoIncrement, pk, semicolon);
    }

    return "ALTER TABLE "
        + tableName
        + " ADD "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  /**
   * Generates the SQL statement to modify a column in the specified table
   *
   * @param tableName The table to add
   * @param v The column defined as a value
   * @param tk the name of the technical key field
   * @param useAutoIncrement whether this field uses auto increment
   * @param pk the name of the primary key field
   * @param semicolon whether to add a semicolon behind the statement.
   * @return the SQL statement to modify a column in the specified table
   */
  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    if (databaseDialect != null) {
      return databaseDialect.getModifyColumnStatement(
          tableName, v, tk, useAutoIncrement, pk, semicolon);
    }
    return "ALTER TABLE "
        + tableName
        + " MODIFY "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  @Override
  public boolean isTrinoVariant() {
    return true;
  }

  @Override
  public void addDefaultOptions() {
    setSupportsBooleanDataType(true);
    setSupportsTimestampDataType(true);
  }

  @Override
  public String getLegacyColumnName(
      DatabaseMetaData dbMetaData, ResultSetMetaData rsMetaData, int index)
      throws HopDatabaseException {
    return super.getLegacyColumnName(dbMetaData, rsMetaData, index);
  }
}
