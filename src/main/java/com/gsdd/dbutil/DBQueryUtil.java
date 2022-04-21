package com.gsdd.dbutil;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DBQueryUtil {

    /**
     * Allow to check if DB exists. Try to extract DB metadata.
     *
     * @param mainTable some existing table on DB.
     * @param driver connector for DB.
     * @param url
     * @param user
     * @param pass
     * @return true if DB exists.
     */
    public static boolean dbExist(
            String mainTable, String driver, String url, String user, String pass) {
        boolean exists = false;
        try {
            if (DBConnection.getInstance().getCon() == null) {
                DBConnection.getInstance().connectDB(driver, url, user, pass);
            }
            exists = dbCheckMetadata(mainTable);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return exists;
    }

    private static boolean dbCheckMetadata(String mainTable) {
        boolean check = false;
        try {
            DatabaseMetaData metaData = DBConnection.getInstance().getCon().getMetaData();
            DBConnection.getInstance().setSt(DBConnection.getInstance().getCon().createStatement());
            DBConnection.getInstance()
                    .setRs(
                            metaData.getTables(
                                    DBConnection.getInstance().getCon().getCatalog(),
                                    "APP",
                                    mainTable,
                                    new String[] {"TABLE"}));
            check = DBConnection.getInstance().getRs().next();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } finally {
            DBConnection.getInstance().closeQuery();
        }
        return check;
    }
}
