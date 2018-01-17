package com.feng.sparkproject.test;

import com.feng.sparkproject.conf.ConfigurationManager;

/**
 * @Description: 配置管理组件测试类
 * @author feng
 * @date 2018年1月16日
 */
public class ConfigurationManagerTest {

	public static void main(String[] args) {
		String testkey1 = ConfigurationManager.getProperty("jdbc.driver");
		String testkey2 = ConfigurationManager.getProperty("jdbc.url");  
		System.out.println(testkey1); 
		System.out.println(testkey2);  
	}
	
}
