package com.feng.sparkproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;

import com.alibaba.fastjson.JSONObject;
import com.feng.sparkproject.constant.Constants;
import com.feng.sparkproject.dao.ISessionAggrStatDAO;
import com.feng.sparkproject.dao.ISessionDetailDAO;
import com.feng.sparkproject.dao.ISessionRandomExtractDAO;
import com.feng.sparkproject.dao.ITaskDAO;
import com.feng.sparkproject.dao.factory.DAOFactory;
import com.feng.sparkproject.domain.SessionAggrStat;
import com.feng.sparkproject.domain.SessionDetail;
import com.feng.sparkproject.domain.SessionRandomExtract;
import com.feng.sparkproject.domain.Task;
import com.feng.sparkproject.test.MockData;
import com.feng.sparkproject.util.DateUtils;
import com.feng.sparkproject.util.NumberUtils;
import com.feng.sparkproject.util.ParamUtils;
import com.feng.sparkproject.util.SparkUtils;
import com.feng.sparkproject.util.StringUtils;
import com.feng.sparkproject.util.ValidUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
    // 测试
    args = new String[] {"2"};
    long taskid = ParamUtils.getTaskIdFromArgs(args);
    Task task = taskDAO.findById(taskid);
    if (task == null) {
      System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
      return;
    }
    JSONObject taskParm = JSONObject.parseObject(task.getTaskParam());
    // 获取指定日期范围内的用户行为数据RDD
    JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParm);
    JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
    System.out.println("sessionid2actionRDD count:" + sessionid2actionRDD.count());
    sessionid2actionRDD.take(20).forEach(tuple -> {
      System.out.println("aaaaa:" + tuple._1 + ":" + tuple._2);
    });

    // <sessionid,(sessionid,<action userinfo>>)
    JavaPairRDD<String, String> sessionid2AggrInfoRDD =
        aggregateBySession(spark, sessionid2actionRDD);

    sessionid2AggrInfoRDD.take(20).forEach(tuple -> {
      System.out.println("sessionid2AggrInfoRDD:" + tuple._1 + ":" + tuple._2);
    });
    
     AccumulatorV2<String, String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
     spark.sparkContext().register(sessionAggrStatAccumulator);
    
    
     // 过滤并统计
     JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
     filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParm, sessionAggrStatAccumulator);
    
     // <sessionid,actioninfo>
     JavaPairRDD<String, Row> sessionid2detailRDD =
     getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
    
     // 随机抽取并入库
     randomExtractSession(spark, task.getTaskid(), filteredSessionid2AggrInfoRDD,
     sessionid2detailRDD);
    
     // 计算出各个范围的session占比，并写入MySQL
     calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());
    spark.close();
  }


  private static void randomExtractSession(SparkSession spark, long taskid,
      JavaPairRDD<String, String> sessionid2AggrInfoRDD,
      JavaPairRDD<String, Row> sessionid2actionRDD) {
    // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(tuple -> {
      String aggrInfo = tuple._2;

      String startTime =
          StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
      String dateHour = DateUtils.getDateHour(startTime);

      return new Tuple2<String, String>(dateHour, aggrInfo);
    });

    Map<String, Long> countMap = time2sessionidRDD.countByKey();
    // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

    for (Map.Entry<String, Long> countEntry : countMap.entrySet()) {
      String dateHour = countEntry.getKey();
      String date = dateHour.split("_")[0];
      String hour = dateHour.split("_")[1];

      long count = Long.valueOf(String.valueOf(countEntry.getValue()));

      Map<String, Long> hourCountMap = dateHourCountMap.get(date);
      if (hourCountMap == null) {
        hourCountMap = new HashMap<String, Long>();
        dateHourCountMap.put(date, hourCountMap);
      }

      hourCountMap.put(hour, count);
    }


    // 总共要抽取100个session，先按照天数，进行平分
    int extractNumberPerDay = 100 / dateHourCountMap.size();

    // <date,<hour,(3,5,20,102)>>:数字代表要抽取第几个

    Map<String, Map<String, List<Integer>>> dateHourExtractMap =
        new HashMap<String, Map<String, List<Integer>>>();

    Random random = new Random();

    for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
      String date = dateHourCountEntry.getKey();
      Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

      // 计算出这一天的session总数
      long sessionCount = 0L;
      for (long hourCount : hourCountMap.values()) {
        sessionCount += hourCount;
      }

      Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
      if (hourExtractMap == null) {
        hourExtractMap = new HashMap<String, List<Integer>>();
        dateHourExtractMap.put(date, hourExtractMap);
      }

      // 遍历每个小时
      for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
        String hour = hourCountEntry.getKey();
        long count = hourCountEntry.getValue();

        // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
        // 就可以计算出，当前小时需要抽取的session数量
        int hourExtractNumber =
            (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
        if (hourExtractNumber > count) {
          hourExtractNumber = (int) count;
        }

        // 先获取当前小时的存放随机数的list
        List<Integer> extractIndexList = hourExtractMap.get(hour);
        if (extractIndexList == null) {
          extractIndexList = new ArrayList<Integer>();
          hourExtractMap.put(hour, extractIndexList);
        }

        // 生成上面计算出来的数量的随机数
        for (int i = 0; i < hourExtractNumber; i++) {
          int extractIndex = random.nextInt((int) count);
          while (extractIndexList.contains(extractIndex)) {
            extractIndex = random.nextInt((int) count);
          }
          extractIndexList.add(extractIndex);
        }
      }
    }

    Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
        new HashMap<String, Map<String, IntList>>();

    for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap
        .entrySet()) {
      String date = dateHourExtractEntry.getKey();
      Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

      Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

      for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
        String hour = hourExtractEntry.getKey();
        List<Integer> extractList = hourExtractEntry.getValue();

        IntList fastutilExtractList = new IntArrayList();

        for (int i = 0; i < extractList.size(); i++) {
          fastutilExtractList.add(extractList.get(i));
        }

        fastutilHourExtractMap.put(hour, fastutilExtractList);
      }

      fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
    }

    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
        sc.broadcast(fastutilDateHourExtractMap);

    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
    JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

    // 用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
    // 并抽取session，直接写入MySQL的random_extract_session表
    // 用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
    JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(tuple -> {
      List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

      String dateHour = tuple._1;
      String date = dateHour.split("_")[0];
      String hour = dateHour.split("_")[1];
      Iterator<String> iterator = tuple._2.iterator();

      /**
       * 使用广播变量的时候 直接调用广播变量（Broadcast类型）的value() / getValue() 可以获取到之前封装的广播变量
       */
      Map<String, Map<String, IntList>> dateHourExtractMapValue =
          dateHourExtractMapBroadcast.value();
      List<Integer> extractIndexList = dateHourExtractMapValue.get(date).get(hour);

      ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

      int index = 0;
      while (iterator.hasNext()) {
        String sessionAggrInfo = iterator.next();

        if (extractIndexList.contains(index)) {
          String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
              Constants.FIELD_SESSION_ID);

          // 将数据写入MySQL
          SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
          sessionRandomExtract.setTaskid(taskid);
          sessionRandomExtract.setSessionid(sessionid);
          sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
              "\\|", Constants.FIELD_START_TIME));
          sessionRandomExtract.setSearchKeywords(StringUtils
              .getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
          sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

          sessionRandomExtractDAO.insert(sessionRandomExtract);

          // 将sessionid加入list
          extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
        }

        index++;
      }

      return extractSessionids.iterator();
    });

    /**
     * 获取抽取出来的session的明细数据
     */
    JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
        extractSessionidsRDD.join(sessionid2actionRDD);

    extractSessionDetailRDD.foreach(tuple -> {
      Row row = tuple._2._2;

      SessionDetail sessionDetail = new SessionDetail();
      sessionDetail.setTaskid(taskid);
      sessionDetail.setUserid(row.getLong(1));
      sessionDetail.setSessionid(row.getString(2));
      sessionDetail.setPageid(row.getLong(3));
      sessionDetail.setActionTime(row.getString(4));
      sessionDetail.setSearchKeyword(row.getString(5));
      sessionDetail.setClickCategoryId(row.getLong(6));
      sessionDetail.setClickProductId(row.getLong(7));
      sessionDetail.setOrderCategoryIds(row.getString(8));
      sessionDetail.setOrderProductIds(row.getString(9));
      sessionDetail.setPayCategoryIds(row.getString(10));
      sessionDetail.setPayProductIds(row.getString(11));

      ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
      sessionDetailDAO.insert(sessionDetail);
    });
  }


  private static void calculateAndPersistAggrStat(String value, long taskid) {
    // 从Accumulator统计串中获取值
    long session_count =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

    long visit_length_1s_3s = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
    long visit_length_4s_6s = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
    long visit_length_7s_9s = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
    long visit_length_10s_30s = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
    long visit_length_30s_60s = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
    long visit_length_1m_3m = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
    long visit_length_3m_10m = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
    long visit_length_10m_30m = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
    long visit_length_30m =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

    long step_length_1_3 =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
    long step_length_4_6 =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
    long step_length_7_9 =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
    long step_length_10_30 = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
    long step_length_30_60 = Long
        .valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
    long step_length_60 =
        Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

    // 计算各个访问时长和访问步长的范围
    double visit_length_1s_3s_ratio =
        NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count, 2);
    double visit_length_4s_6s_ratio =
        NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count, 2);
    double visit_length_7s_9s_ratio =
        NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count, 2);
    double visit_length_10s_30s_ratio =
        NumberUtils.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
    double visit_length_30s_60s_ratio =
        NumberUtils.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
    double visit_length_1m_3m_ratio =
        NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count, 2);
    double visit_length_3m_10m_ratio =
        NumberUtils.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
    double visit_length_10m_30m_ratio =
        NumberUtils.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
    double visit_length_30m_ratio =
        NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

    double step_length_1_3_ratio =
        NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
    double step_length_4_6_ratio =
        NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
    double step_length_7_9_ratio =
        NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
    double step_length_10_30_ratio =
        NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
    double step_length_30_60_ratio =
        NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
    double step_length_60_ratio =
        NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

    // 将统计结果封装为Domain对象
    SessionAggrStat sessionAggrStat = new SessionAggrStat();
    sessionAggrStat.setTaskid(taskid);
    sessionAggrStat.setSession_count(session_count);
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

    // 调用对应的DAO插入统计结果
    ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
    sessionAggrStatDAO.insert(sessionAggrStat);
  }


  private static JavaPairRDD<String, Row> getSessionid2detailRDD(
      JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
      JavaPairRDD<String, Row> sessionid2actionRDD) {
    JavaPairRDD<String, Row> sessionid2detailRDD =
        filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD)
            .mapToPair(tuple -> new Tuple2<String, Row>(tuple._1, tuple._2._2));
    return sessionid2detailRDD;
  }

  /**
   * @Description: 过滤session数据，并进行聚合统计
   */
  private static JavaPairRDD<String, String> filterSessionAndAggrStat(
      JavaPairRDD<String, String> sessionid2AggrInfoRDD, JSONObject taskParm,
      AccumulatorV2<String, String> sessionAggrStatAccumulator) {
    String startAge = ParamUtils.getParam(taskParm, Constants.PARAM_START_AGE);
    String endAge = ParamUtils.getParam(taskParm, Constants.PARAM_END_AGE);
    String professionals = ParamUtils.getParam(taskParm, Constants.PARAM_PROFESSIONALS);
    String cities = ParamUtils.getParam(taskParm, Constants.PARAM_CITIES);
    String sex = ParamUtils.getParam(taskParm, Constants.PARAM_SEX);
    String keywords = ParamUtils.getParam(taskParm, Constants.PARAM_KEYWORDS);
    String categoryIds = ParamUtils.getParam(taskParm, Constants.PARAM_CATEGORY_IDS);

    // 拼接查询参数
    String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
        + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
        + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
        + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
        + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
        + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
        + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

    // 去掉最后的|
    if (_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length() - 1);
    }
    final String parameter = _parameter;
    // 根据筛选参数进行过滤
    JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
        sessionid2AggrInfoRDD.filter(tuple -> {

          String aggrInfo = tuple._2;

          // 接着，依次按照筛选条件进行过滤
          // 按照年龄范围进行过滤（startAge、endAge）
          if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
              Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
            return false;
          }

          if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
              Constants.PARAM_PROFESSIONALS)) {
            return false;
          }

          if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
            return false;
          }

          if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
            return false;
          }

          // 按照搜索词进行过滤
          if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
              Constants.PARAM_KEYWORDS)) {
            return false;
          }

          if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
              Constants.PARAM_CATEGORY_IDS)) {
            return false;
          }

          // 对session的访问时长和访问步长，进行统计，根据session对应的范围
          // 进行相应的累加计数
          sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

          // 计算出session的访问时长和访问步长的范围，并进行相应的累加
          long visitLength = Long.valueOf(
              StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
          long stepLength = Long.valueOf(
              StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
          calculateVisitLength(visitLength, sessionAggrStatAccumulator);
          calculateStepLength(stepLength, sessionAggrStatAccumulator);
          return true;

        });
    return filteredSessionid2AggrInfoRDD;
  }

  /**
   * 计算访问时长范围
   * 
   * @param visitLength
   */
  private static void calculateVisitLength(long visitLength,
      AccumulatorV2<String, String> sessionAggrStatAccumulator) {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
    } else if (visitLength > 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
    }
  }

  /**
   * 计算访问步长范围
   * 
   * @param stepLength
   */
  private static void calculateStepLength(long stepLength,
      AccumulatorV2<String, String> sessionAggrStatAccumulator) {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
    } else if (stepLength > 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
    }
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
   * @return session粒度聚合数据(sessionid,"sessionid|searchKeywords|clickCategoryIds|visitLength|stepLength|startTime|age|professional|city|sex")
   */
  private static JavaPairRDD<String, String> aggregateBySession(SparkSession spark,
      JavaPairRDD<String, Row> sessinoid2actionRDD) {
    // 对行为数据按session粒度进行分组
    JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessinoid2actionRDD.groupByKey();

    // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
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
        String searchKeyword = row.get(5)!=null?row.getString(5):null;
        Long clickCategoryId = row.get(6)!=null?row.getLong(6):null;

        // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
        // 其实，只有搜索行为，是有searchKeyword字段的
        // 只有点击品类的行为，是有clickCategoryId字段的
        // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

        // 我们决定是否将搜索词或点击品类id拼接到字符串中去
        // 首先要满足：不能是null值
        // 其次，之前的字符串中还没有搜索词或者点击品类id

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

      // 我们返回的数据格式，即使<sessionid,partAggrInfo>
      // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
      // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
      // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
      // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
      // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

      // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
      // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
      // 然后再直接将返回的Tuple的key设置成sessionid
      // 最后的数据格式，还是<sessionid,fullAggrInfo>

      // 聚合数据，用什么样的格式进行拼接？
      // 我们这里统一定义，使用key=value|key=value
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
    JavaPairRDD<Long, Row> userid2InfoRDD =
        userInfoRDD.mapToPair(row -> new Tuple2<Long, Row>(row.getLong(0), row));
    /**
     * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
     * 
     * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据 userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
     * 
     */

    // 将session粒度聚合数据，与用户信息进行join
    JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
        userid2PartAggrInfoRDD.join(userid2InfoRDD);
    System.out.println("userid2FullInfoRDD:"+userid2FullInfoRDD.count());
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
