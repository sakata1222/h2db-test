package jp.gr.java_conf.saka.h2dbtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.h2.engine.Constants;
import org.junit.jupiter.api.Test;

class MultiThreadAccessTest {

  private static final String URL = "jdbc:h2:mem:test_concurrent;LOCK_TIMEOUT=120000";

  @Test
  void test() throws SQLException, InterruptedException, ExecutionException {
    Locale.setDefault(Locale.ENGLISH);
    Connection instanceKeepConnection = DriverManager.getConnection(URL);
    ExecutorService threadPool = Executors.newFixedThreadPool(2);
    try {
      List<String> tables = Arrays.asList("TABLE1", "TABLE2");
      System.out.println("creating tables");
      try (Connection con = DriverManager.getConnection(URL);
        Statement statement = con.createStatement()) {
        for (String table : tables) {
          statement.execute("CREATE TABLE " + table + " (COL1 varchar(20));");
        }
      }

      System.out.println("inserting dummy data (it takes several tens of seconds to insert)");
      List<Future<Void>> insertFuture = new ArrayList<>();
      for (String table : tables) {
        insertFuture.add(threadPool.submit(() -> {
          String insertTemplate = "INSERT INTO " + table + " (COL1) VALUES(?)";
          try (Connection con = DriverManager.getConnection(URL);
            PreparedStatement preparedStatement = con.prepareStatement(insertTemplate)) {
            for (int i = 0; i < 3000000; i++) {
              preparedStatement.setString(1, String.valueOf(i));
              preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
          }
          return null;
        }));
      }
      for (Future f : insertFuture) {
        f.get();
      }
      try (Statement statement = instanceKeepConnection.createStatement()) {
        statement.execute("SET TRACE_LEVEL_SYSTEM_OUT 3");
      }
      System.out.println("creating indexes");
      List<Future<Void>> createIndexFuture = new ArrayList<>();
      for (String table : tables) {
        createIndexFuture.add(threadPool.submit(() -> {
          try (Connection con = DriverManager.getConnection(URL);
            Statement statement = con.createStatement()) {
            statement.execute(
              "CREATE INDEX TEST_" + table + " ON " + table + " (COL1);");
          }
          return null;
        }));
      }
      Collections.reverse(createIndexFuture);
      for (Future f : createIndexFuture) {
        f.get(); // Fail to CREATE INDEX in H2DB 1.4.199
      }
    } finally {
      instanceKeepConnection.close();
      threadPool.shutdown();
    }
  }
}
