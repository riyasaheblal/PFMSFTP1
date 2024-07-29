package utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Properties;



public class GenericUtilities {

	
	public static HashMap<String,String> accountCategoryCode = new HashMap<String,String>();
	
	static {
		accountCategoryCode.put("01","IND");
		accountCategoryCode.put(" ","UN");
		accountCategoryCode.put("0202","COS ");
		accountCategoryCode.put("0210","HUF ");
		accountCategoryCode.put("0207","JLG ");
		accountCategoryCode.put("020502","PF");
		accountCategoryCode.put("020302","PCS ");
		accountCategoryCode.put("020501","PC");
		accountCategoryCode.put("0206","SGD ");
		accountCategoryCode.put("0201","TG");
	}
	
	
	public  static String printException(Exception e) 
	{
		try {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			return sw.toString();
		}
		catch(Exception e2) {
			e2.printStackTrace();
			return "Unable to Handle Exception";

		}
	}
	
	public  static Properties getPropertyFile(String propertyFileName){
		
		Properties properties = new Properties();
		
		try {
			
			
			/*InputStream inputStream = DatabaseMaster.class.getResourceAsStream("/properties/"+propertyFileName);
			properties.load(inputStream);
			inputStream.close();*/
			
			
//			FileInputStream fin=	new FileInputStream("/cpsmsftp/properties/"+propertyFileName); // RRB/WB/NB
			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+propertyFileName); // RRB/WB/NB	
			
			properties.load(fin);
			fin.close();
			
		} catch (IOException e) {
			System.out.print(printException(e));
		}
		
		return properties;
		
		
		
		
	}
	
	
	
	
	
	
	
	
//	
//	public static Properties BanksNameProp = new Properties();
//	static
//	{
//	try {
////			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"BanksName.properties"); // RRB/WB/NB
//			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"BanksName.properties"); // RRB/WB/NB
//					
//			BanksNameProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
//	public static Properties CPSMSProp = new Properties();
//	static
//	{
//	try {
////			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"CPSMS.properties"); // RRB/WB/NB
//			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"CPSMS.properties"); // RRB/WB/NB	
//			CPSMSProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
	
	public static Properties databaseProp = new Properties();
	static
	{
	try {
//			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"database.properties"); // RRB/WB/NB
		FileInputStream fin=	new FileInputStream("D:\\Data\\New folder\\NEW_SFTP_MIGRATION\\src\\properties\\"+"database.properties"); // RRB/WB/NB
			
						
			databaseProp.load(fin);
			fin.close();
			
		} catch (IOException e) {
			System.out.print(printException(e));
		}
		
		
	}
//	
//	public static Properties ePaymentProp = new Properties();
//	static
//	{
//	try {
////			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"ePayment.properties"); // RRB/WB/NB
//			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"ePayment.properties"); // RRB/WB/NB
//			
//			
//			ePaymentProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
//	
//	public static Properties ServerInfoProp = new Properties();
//	static
//	{
//	try {
////			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"ServerInfo.properties"); // RRB/WB/NB
//			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"ServerInfo.properties"); // RRB/WB/NB
//			
//			ServerInfoProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
//	
//	public static Properties SFTPBankCodeProp = new Properties();
//	
//	static
//	{
//	try {
////			FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"SFTPBankCode.properties"); // RRB/WB/NB
//			FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"SFTPBankCode.properties"); // RRB/WB/NB
//			
//			
//			SFTPBankCodeProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
//	
//	public static Properties SFTPServerIdetifyProp = new Properties();
//	static
//	{
//	try {
////		    FileInputStream fin=	new FileInputStream("D:/odhisa_properties/AccountValidation/"+"SFTPServerIdetify.properties"); // RRB/WB/NB
//		    FileInputStream fin=	new FileInputStream("/pfms/520cpsms/properties/AccountValidation/"+"SFTPServerIdetify.properties"); // RRB/WB/NB
//			
//			
//			SFTPServerIdetifyProp.load(fin);
//			fin.close();
//			
//		} catch (IOException e) {
//			System.out.print(printException(e));
//		}
//		
//		
//	}
	
	
	
	
	
	
}
