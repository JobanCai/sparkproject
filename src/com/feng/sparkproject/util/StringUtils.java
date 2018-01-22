package com.feng.sparkproject.util;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

/**
 * @Description: 字符串工具类
 * @author feng
 * @date 2018年1月16日
 */
public class StringUtils {

  /**
   * 判断字符串是否为空
   * 
   * @param str 字符串
   * @return 是否为空
   */
  public static boolean isEmpty(String str) {
    return str == null || "".equals(str);
  }

  /**
   * 判断字符串是否不为空
   * 
   * @param str 字符串
   * @return 是否不为空
   */
  public static boolean isNotEmpty(String str) {
    return str != null && !"".equals(str);
  }

  /**
   * 截断字符串两侧的逗号
   * 
   * @param str 字符串
   * @return 字符串
   */
  public static String trimComma(String str) {
    if (str.startsWith(",")) {
      str = str.substring(1);
    }
    if (str.endsWith(",")) {
      str = str.substring(0, str.length() - 1);
    }
    return str;
  }

  /**
   * 补全两位数字
   * 
   * @param str
   * @return
   */
  public static String fulfuill(String str) {
    if (str.length() == 2) {
      return str;
    } else {
      return "0" + str;
    }
  }

  /**
   * 从拼接的字符串中提取字段
   * 
   * @param str 字符串
   * @param delimiter 分隔符
   * @param field 字段
   * @return 字段值
   */
  public static String getFieldFromConcatString(String str, String delimiter, String field) {
    String[] fields = str.split(delimiter);
    for (String concatField : fields) {
      String fieldName = concatField.split("=")[0];
      String fieldValue = concatField.split("=")[1];
      if (fieldName.equals(field)) {
        return fieldValue;
      }
    }
    return null;
  }


  public static String mergeResultString(String s1, String s2, String delimiter) {
    String[] s1Fields = s1.split(delimiter);
    String[] s2Fields = s2.split(delimiter);
    StringBuffer buffer = new StringBuffer("");
    assert(s1.length()==s2.length());
    for (int i = 0; i < s1Fields.length; i++) {
      String fieldName = s1Fields[i].split("=")[0];
      String s1FieldValue = s1Fields[i].split("=")[1];
      String s2FieldValue = s2Fields[i].split("=")[1];
      int newValue = Integer.valueOf(s1FieldValue)+Integer.valueOf(s2FieldValue);
      String concatField = fieldName + "=" + newValue;
      buffer.append(concatField);
      if (i<s1Fields.length-1) {
        buffer.append("|");
      }
    }
    return buffer.toString();
  }

  /**
   * 从拼接的字符串中给字段设置值
   * 
   * @param str 字符串
   * @param delimiter 分隔符
   * @param field 字段名
   * @param newFieldValue 新的field值
   * @return 字段值
   */
  public static String setFieldInConcatString(String str, String delimiter, String field,
      String newFieldValue) {
    String[] fields = str.split(delimiter);

    for (int i = 0; i < fields.length; i++) {
      String fieldName = fields[i].split("=")[0];
      if (fieldName.equals(field)) {
        String concatField = fieldName + "=" + newFieldValue;
        fields[i] = concatField;
        break;
      }
    }

    StringBuffer buffer = new StringBuffer("");
    for (int i = 0; i < fields.length; i++) {
      buffer.append(fields[i]);
      if (i < fields.length - 1) {
        buffer.append("|");
      }
    }

    return buffer.toString();
  }

}
