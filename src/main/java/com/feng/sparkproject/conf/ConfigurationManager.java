package com.feng.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @Description: 配置类
 * @author feng
 * @date 2018年1月16日
 */
public class ConfigurationManager {
  private static Logger LOG = Logger.getLogger(ConfigurationManager.class);

  private static Properties prop = new Properties();

  static {
    PropertyConfigurator.configure("target/classes/resources/log4j.properties");
    try {
      InputStream in = ConfigurationManager.class.getClassLoader()
          .getResourceAsStream("resources/project.properties");
      LOG.info(ConfigurationManager.class.getClassLoader().getResource("").getPath());
      LOG.info(ConfigurationManager.class.getResource("").getPath());
      prop.load(in);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public static String getProperty(String key) {
    return prop.getProperty(key);
  }

  /**
   * 获取整数类型的配置项
   * 
   * @param key
   * @return value
   */
  public static Integer getInteger(String key) {
    String value = getProperty(key);
    try {
      return Integer.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  /**
   * 获取布尔类型的配置项
   * 
   * @param key
   * @return value
   */
  public static Boolean getBoolean(String key) {
    String value = getProperty(key);
    try {
      return Boolean.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * 获取Long类型的配置项
   * 
   * @param key
   * @return
   */
  public static Long getLong(String key) {
    String value = getProperty(key);
    try {
      return Long.valueOf(value);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0L;
  }

}
