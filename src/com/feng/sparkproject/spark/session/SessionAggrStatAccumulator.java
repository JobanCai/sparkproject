package com.feng.sparkproject.spark.session;

import org.apache.spark.util.AccumulatorV2;

import com.feng.sparkproject.constant.Constants;
import com.feng.sparkproject.util.StringUtils;

/**
 * @Description: 使用时需要sc.register(accumulator)
 * @author feng
 * @date 2018年1月22日
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

  /**
   * @Fields serialVersionUID : TODO
   */
  private static final long serialVersionUID = 1L;

  // 初始值
  private String result = getInitValue();

  @Override
  public void add(String v) {
    String tempResult = result;
    String field = v;
    if (StringUtils.isNotEmpty(tempResult) && StringUtils.isNotEmpty(field)) {
      String newResult = "";
      // 从tempResult中，提取field对应的值，并累加
      String oldValue = StringUtils.getFieldFromConcatString(tempResult, "\\|", field);
      if (oldValue != null) {
        String newValue = (Long.valueOf(oldValue) + 1) + "";
        newResult = StringUtils.setFieldInConcatString(tempResult, "\\|", field, String.valueOf(newValue));
      }
      result = newResult;
    }
  }

  @Override
  public AccumulatorV2<String, String> copy() {
    SessionAggrStatAccumulator s = new SessionAggrStatAccumulator();
    s.result = this.result;
    return s;
  }

  /*
   * Returns if this accumulator is zero value or not.
   */
  @Override
  public boolean isZero() {
    return result.equals(getInitValue());
//    return true;
  }

  /* 
   * 合并另一个类型相同的累加器
   */
  @Override
  public void merge(AccumulatorV2<String, String> v2) {
    if (v2 instanceof SessionAggrStatAccumulator) {
      this.result = StringUtils.mergeResultString(this.result,((SessionAggrStatAccumulator) v2).result,"\\|");
    }
  }

  @Override
  public void reset() {
    this.result = getInitValue();
  }

  @Override
  public String value() {
    return result;
  }

  private String getInitValue() {
    return Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|"
        + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|"
        + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|"
        + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|"
        + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|"
        + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|"
        + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|"
        + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0";
  }
}
