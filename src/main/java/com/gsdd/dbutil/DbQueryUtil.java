package com.gsdd.dbutil;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DbQueryUtil {

  /**
   * Allow to check if DB exists. Try to extract DB metadata.
   *
   * @param mainTable some existing table on DB.
   * @param db database connection object
   * @return true if DB exists.
   */
  public static boolean dbExist(String mainTable, DbConnection db) {
    boolean exists = false;
    try {
      exists = dbCheckMetadata(mainTable, db);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
    return exists;
  }

  private static boolean dbCheckMetadata(String mainTable, DbConnection db) {
    boolean check = false;
    try {
      DatabaseMetaData metaData = db.getCon().getMetaData();
      db.setSt(db.getCon().createStatement());
      db.setRs(
          metaData.getTables(db.getCon().getCatalog(), "APP", mainTable, new String[] {"TABLE"}));
      check = db.getRs().next();
    } catch (SQLException e) {
      log.error(e.getMessage(), e);
    } finally {
      db.closeQuery();
    }
    return check;
  }
}
