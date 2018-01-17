package com.feng.sparkproject.spark.session;

import org.apache.spark.sql.SparkSession;

import com.feng.sparkproject.constant.Constants;
import com.feng.sparkproject.test.MockData;

/**
 * 用户访问session分析Spark作业
 * 
 * @author Administrator
 *
 */
@SuppressWarnings("unused")
public class UserVisitSessionAnalyzeSpark {

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).master("local")
            // spark sql元数据仓库地址
            .config("spark.sql.warehouse.dir", "E:\\scale\\sparkSQLDemo1").enableHiveSupport()
            .getOrCreate();
    mockData(spark);
    spark.close();
  }

  /**
   * 生成模拟数据（只有本地模式，才会去生成模拟数据）
   * 
   * @param sc
   * @param sqlContext
   */
  private static void mockData(SparkSession spark) {
    MockData.mock(spark);
  }

}
