package dao;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;





import org.apache.log4j.Logger;





import utilities.GenericUtilities;

public class DatabaseMaster 
{
	public static final Logger logger = Logger.getLogger(DatabaseMaster.class);
	public static Connection getConnection(String bankCode)
	{
		Properties properties = new Properties();
		Connection connection = null; 
		try {

			properties= GenericUtilities.databaseProp;

			/*String dataBaseServer=properties.getProperty("AppDataBaseServer"+bankCode);
			String userName=properties.getProperty("AppUserName"+bankCode);
			String password=properties.getProperty("AppPassword"+bankCode);*/

			String dataBaseServer = properties.getProperty("DatabaseServerForAllApp");
			String userName = properties.getProperty("UsernameForAllApp");
			String password = properties.getProperty("PasswordForAllApp");
			String Driver=properties.getProperty("Driver");

			Class.forName(Driver);
			connection = DriverManager.getConnection(dataBaseServer,userName,password);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			logger.info(GenericUtilities.printException(e));
			logger.error(GenericUtilities.printException(e));
		}  
		return connection;
	}

	public static  boolean FileInformation(String bankCode, String filename,String value,Connection connection)
	{
		try {
			if (connection==null ||connection.isClosed())
			{
			 connection =  DatabaseMaster.getConnection(bankCode);
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean isDone = false;
		PreparedStatement preparedStatement = null;
		String query = null;
		try
		{
			if(filename.contains(".xml")||filename.contains(".XML"))
			{
				query="INSERT INTO PSFTPFileInfo(REQ_RES,DWN_UPLOAD,bankcode,dwn_creatdate) values(?,?,?,systimestamp)";
				preparedStatement = connection.prepareStatement(query);
					
					preparedStatement.setString(1, filename);
					preparedStatement.setString(2, value);
					preparedStatement.setString(3, bankCode);
				
				logger.info("Update Query= "+query);
//				preparedStatement = connection.prepareStatement(query);
				System.out.println("query"+query);
				
			}
			logger.info("upload Query= "+query);
			preparedStatement.executeUpdate();
			isDone =true;



		}catch( Exception se ){
			se.printStackTrace();
			logger.error(GenericUtilities.printException(se));
			logger.info(GenericUtilities.printException(se));
			logger.error("Exception while inserting  AGENCYACCOUNTMASTER for bank code :"+bankCode);
		} 
		finally{
			try {
				preparedStatement.close();
//				connection.close();

			} catch (Exception e) {
				logger.error(GenericUtilities.printException(e));
				logger.info(GenericUtilities.printException(e));
				logger.error("Exception while inserting  AGENCYACCOUNTMASTER for bank code :"+bankCode);
			}
		}
		return isDone;

	}



}