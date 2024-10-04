package com.gsdd.dbutil;

import com.gsdd.constants.DbConstants;
import com.gsdd.constants.GralConstants;
import com.gsdd.constants.LoadConstants;
import com.gsdd.exception.TechnicalException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DbConnection {

  private static final DbConnection INSTANCE = new DbConnection();
  private static final Pattern SEMICOLON_PATTERN = Pattern.compile(GralConstants.SEMICOLON);
  private Connection con;
  private PreparedStatement pst;
  private Statement st;
  private ResultSet rs;

  public void connectDB(String driver, String url, String user, String pass) {
    try {
      Class.forName(driver);
      this.con = DriverManager.getConnection(url, user, pass);
    } catch (Exception e) {
      throw new TechnicalException(e);
    }
  }

  private File getImportFile(URL path) {
    File importFile = null;
    try {
      if (DbConstants.FILE.equals(path.getProtocol())) {
        importFile = new File(path.toURI());
        log.info(
            "GetImportFile on protocol {} is {}", DbConstants.FILE, importFile.getAbsolutePath());
      } else {
        File pos = new File(GralConstants.DOT).getAbsoluteFile();
        log.info("GetImportFile us {}", pos.getAbsolutePath());
        importFile =
            new File(
                pos.getAbsolutePath()
                        .substring(0, pos.getAbsolutePath().lastIndexOf(GralConstants.DOT))
                    + LoadConstants.IMPORT);
      }
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return importFile;
  }

  /**
   * Allows to process a sql file for execute all the statements on it. Normally for init/update a
   * DB.
   *
   * @param throwExceptionFlag if true then it stops the execution on any bad statement.
   */
  public void executeImport(Boolean throwExceptionFlag) {
    try {
      StringBuilder importSQL = readImportFile();
      String[] statements = SEMICOLON_PATTERN.split(importSQL.toString());
      executeImportScript(statements, throwExceptionFlag);
    } catch (Exception e) {
      throw new TechnicalException(e.getMessage(), e);
    }
  }

  private StringBuilder readImportFile() {
    try {
      URL path = DbConnection.class.getResource(LoadConstants.IMPORT);
      File f = getImportFile(path);
      return processSQL(f);
    } catch (Exception e) {
      throw new TechnicalException(e.getMessage(), e);
    }
  }

  public StringBuilder processSQL(File f) throws IOException {
    String s = GralConstants.EMPTY;
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(f))) {
      while ((s = br.readLine()) != null) {
        sb.append(s);
      }
    }
    return sb;
  }

  private void executeImportScript(String[] statements, boolean throwExceptionFlag)
      throws SQLException {
    if (statements != null) {
      for (String statement : statements) {
        executeQuery(statement, throwExceptionFlag);
      }
    }
  }

  private void executeQuery(String sql, boolean throwExceptionFlag) throws SQLException {
    try {
      st = con.createStatement();
      if (!GralConstants.EMPTY.equals(sql.trim())) {
        st.executeUpdate(sql);
        log.info(GralConstants.SYS_OUT + "{}", sql);
      }
    } catch (SQLException e) {
      log.error(GralConstants.SYS_OUT + "{}", e.getMessage(), e);
      if (throwExceptionFlag) {
        throw e;
      }
    } finally {
      closeQuery();
    }
  }

  /** Close DB objects (preparedstatement, resultset, etc). */
  public void closeQuery() {
    closeQuietly(rs);
    closeQuietly(pst);
    closeQuietly(st);
  }

  public void disconnectDB() {
    closeQuery();
    closeQuietly(con);
  }

  private void closeQuietly(AutoCloseable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (Exception e) {
        log.trace(e.getMessage(), e);
      }
    }
  }

  public static DbConnection getInstance() {
    return INSTANCE;
  }
}
