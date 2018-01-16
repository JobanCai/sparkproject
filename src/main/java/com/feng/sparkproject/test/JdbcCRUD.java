package com.feng.sparkproject.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;


public class JdbcCRUD {

  public static void main(String[] args) {
    preparedStatement();
  }

  /**
   * 测试PreparedStatement
   */
  private static void preparedStatement() {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");

      conn = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/spark-project?characterEncoding=utf8", "root", "feng");

      String sql = "insert into test_user(name,age) values(?,?)";

      pstmt = conn.prepareStatement(sql);

      pstmt.setString(1, "李四");
      pstmt.setInt(2, 26);

      int rtn = pstmt.executeUpdate();

      System.out.println("SQL语句影响了【" + rtn + "】行。");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (pstmt != null) {
          pstmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    }
  }

}
