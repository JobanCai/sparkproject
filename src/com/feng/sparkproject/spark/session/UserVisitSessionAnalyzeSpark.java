package com.feng.sparkproject.spark.session;

import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;
import com.feng.sparkproject.constant.Constants;
import com.feng.sparkproject.dao.ITaskDAO;
import com.feng.sparkproject.dao.factory.DAOFactory;
import com.feng.sparkproject.domain.Task;
import com.feng.sparkproject.test.MockData;
import com.feng.sparkproject.util.DateUtils;
import com.feng.sparkproject.util.ParamUtils;
import com.feng.sparkproject.util.SparkUtils;
import com.feng.sparkproject.util.StringUtils;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

import scala.Tuple2;

/**
 * 用户访问session分析Spark作业
 * 
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 
 * 1、时间范围：起始日期~结束日期 2、性别：男或女 3、年龄范围 4、职业：多选 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
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

    ITaskDAO taskDAO = DAOFactory.getTaskDAO();
    long taskid = ParamUtils.getTaskIdFromArgs(args);
    Task task = taskDAO.findById(taskid);
    if (task == null) {
      System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
      return;
    }
    JSONObject taskParm = JSONObject.parseObject(task.getTaskParam());
    JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParm);
    JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
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


  /**
   * 获取sessionid2到访问行为数据的映射的RDD
   * 
   * @param actionRDD
   * @return
   */
  public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {

    JavaPairRDD<String, Row> sessinoid2actionRDD =
        actionRDD.mapToPair(row -> new Tuple2<String, Row>(row.getString(2), row));

    return sessinoid2actionRDD;
  }

  /**
   * 对行为数据按session粒度进行聚合
   * 
   * @param actionRDD 行为数据RDD
   * @return session粒度聚合数据
   */
  private static JavaPairRDD<String, String> aggregateBySession(SparkSession spark,
      JavaPairRDD<String, Row> sessinoid2actionRDD) {
    // 对行为数据按session粒度进行分组
    JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey();

    // <userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)
    JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(tuple -> {
      String sessionid = tuple._1;
      Iterator<Row> iterator = tuple._2.iterator();

      StringBuffer searchKeywordsBuffer = new StringBuffer("");
      StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

      Long userid = null;

      // session的起始和结束时间
      Date startTime = null;
      Date endTime = null;
      // session的访问步长
      int stepLength = 0;

      // 遍历session所有的访问行为
      while (iterator.hasNext()) {
        // 提取每个访问行为的搜索词字段和点击品类字段
        Row row = iterator.next();
        if (userid == null) {
          userid = row.getLong(1);
        }
        String searchKeyword = row.getString(5);
        Long clickCategoryId = row.getLong(6);

        if (StringUtils.isNotEmpty(searchKeyword)) {
          if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
            searchKeywordsBuffer.append(searchKeyword + ",");
          }
        }
        if (clickCategoryId != null) {
          if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
            clickCategoryIdsBuffer.append(clickCategoryId + ",");
          }
        }

        // 计算session开始和结束时间
        Date actionTime = DateUtils.parseTime(row.getString(4));

        if (startTime == null) {
          startTime = actionTime;
        }
        if (endTime == null) {
          endTime = actionTime;
        }

        if (actionTime.before(startTime)) {
          startTime = actionTime;
        }
        if (actionTime.after(endTime)) {
          endTime = actionTime;
        }

        // 计算session访问步长
        stepLength++;
      }

      String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
      String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

      // 计算session访问时长（秒）
      long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

      // 聚合数据，用key=value|key=value格式拼接
      String partAggrInfo =
          Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS + "="
              + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds
              + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
              + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME
              + "=" + DateUtils.formatTime(startTime);

      return new Tuple2<Long, String>(userid, partAggrInfo);
    });

    // 查询所有用户数据，并映射成<userid,Row>的格式
    String sql = "select * from user_info";
    JavaRDD<Row> userInfoRDD = spark.sql(sql).javaRDD();

    // <userid,Row>
    JavaPairRDD<Long, Row> userid2InfoRDD =
        userInfoRDD.mapToPair(row -> new Tuple2<Long, Row>(row.getLong(0), row));

    // 将session粒度聚合数据，与用户信息进行join
    JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
        userid2PartAggrInfoRDD.join(userid2InfoRDD);

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(tuple -> {
      String partAggrInfo = tuple._2._1;
      Row userInfoRow = tuple._2._2;

      String sessionid =
          StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

      int age = userInfoRow.getInt(3);
      String professional = userInfoRow.getString(4);
      String city = userInfoRow.getString(5);
      String sex = userInfoRow.getString(6);

      String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
          + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "="
          + city + "|" + Constants.FIELD_SEX + "=" + sex;

      return new Tuple2<String, String>(sessionid, fullAggrInfo);
    });
    return sessionid2FullAggrInfoRDD;
  }

}
