package com.feng.sparkproject.dao.factory;

import com.feng.sparkproject.dao.IAdBlacklistDAO;
import com.feng.sparkproject.dao.IAdClickTrendDAO;
import com.feng.sparkproject.dao.IAdProvinceTop3DAO;
import com.feng.sparkproject.dao.IAdStatDAO;
import com.feng.sparkproject.dao.IAdUserClickCountDAO;
import com.feng.sparkproject.dao.IAreaTop3ProductDAO;
import com.feng.sparkproject.dao.IPageSplitConvertRateDAO;
import com.feng.sparkproject.dao.ISessionAggrStatDAO;
import com.feng.sparkproject.dao.ISessionDetailDAO;
import com.feng.sparkproject.dao.ISessionRandomExtractDAO;
import com.feng.sparkproject.dao.ITaskDAO;
import com.feng.sparkproject.dao.ITop10CategoryDAO;
import com.feng.sparkproject.dao.ITop10SessionDAO;
import com.feng.sparkproject.dao.Impl.AdBlacklistDAOImpl;
import com.feng.sparkproject.dao.Impl.AdClickTrendDAOImpl;
import com.feng.sparkproject.dao.Impl.AdProvinceTop3DAOImpl;
import com.feng.sparkproject.dao.Impl.AdStatDAOImpl;
import com.feng.sparkproject.dao.Impl.AdUserClickCountDAOImpl;
import com.feng.sparkproject.dao.Impl.AreaTop3ProductDAOImpl;
import com.feng.sparkproject.dao.Impl.PageSplitConvertRateDAOImpl;
import com.feng.sparkproject.dao.Impl.SessionAggrStatDAOImpl;
import com.feng.sparkproject.dao.Impl.SessionDetailDAOImpl;
import com.feng.sparkproject.dao.Impl.SessionRandomExtractDAOImpl;
import com.feng.sparkproject.dao.Impl.TaskDAOImpl;
import com.feng.sparkproject.dao.Impl.Top10CategoryDAOImpl;
import com.feng.sparkproject.dao.Impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
