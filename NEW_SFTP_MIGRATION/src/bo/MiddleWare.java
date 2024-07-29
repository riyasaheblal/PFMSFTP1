package bo;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import utilities.GenericUtilities;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import dao.DatabaseMaster;

public class MiddleWare 
{
	public static final Logger logger = Logger.getLogger(MiddleWare.class);

	public void processFunction2(String bankcode,Connection connection)
	{

		logger.info("============Start processFunction2==========================");
		LinkedHashMap<Integer, HashMap> map=fetchcommonData(bankcode,connection);
		System.out.println(map);
		LinkedHashMap<String, String> serverInfo=server_details1(bankcode,connection);
		System.out.println(serverInfo);
		if(serverInfo.size()>0)
		{
			if(map.size()>0)
			{

				download_UploadFile(map, bankcode,serverInfo,connection);

			}
		}
		logger.info("============END processFunction2==========================");

	}



	public void download_UploadFile(LinkedHashMap<Integer, HashMap> map, String bankcode, LinkedHashMap<String, String> serverInfo, Connection conn1) {
	    int port = 22;
	    JSch jsch = new JSch();
	    com.jcraft.jsch.Session session = null;
	    Channel channel = null;
	    final ChannelSftp[] channelSftp = new ChannelSftp[1]; // Use an array to hold the reference
	    Vector<ChannelSftp.LsEntry> filelist = null;
	    ExecutorService executorService = Executors.newFixedThreadPool(10); // Adjust the pool size as needed

	    try {
	        for (Integer parameter : map.keySet()) {
	            HashMap<String, String> Data = map.get(parameter);
	            String value = Data.get("FLOW");
	            String home = "/pfmsftp/New/" + bankcode; // Initialize home here

	            if (session == null || !session.isConnected()) {
	            	System.out.println("Server Connection");
	                session = jsch.getSession(serverInfo.get("username"), serverInfo.get("ip"), port);
	                session.setPassword(serverInfo.get("pfms_password"));
	                Properties config = new Properties();
	                config.put("StrictHostKeyChecking", "no");
	                session.setConfig(config);
	                logger.info("Connecting to SFTP server...");
	                session.connect();
	                channel = session.openChannel("sftp");
	                channel.connect();
	                channelSftp[0] = (ChannelSftp) channel;
	            }

	            if (channelSftp[0] != null && channelSftp[0].isConnected()) {
	                if ("Download".equals(value)) {
	                	System.out.println("Its going in"+ value);
	                    String path = Data.get("source");
	                    channelSftp[0].cd(home + path);
	                    filelist = channelSftp[0].ls(home + path);

	                    String downloadPattern = bankcode + "*.xml";
	                    Vector<ChannelSftp.LsEntry> list = channelSftp[0].ls(downloadPattern);
	                    
	                    List<Future<?>> futures = new ArrayList<>();
	                    
	                    for (ChannelSftp.LsEntry oListItem : list) {
	                        if (oListItem.getFilename() != null && oListItem.getFilename().endsWith(".xml") && !oListItem.getFilename().contains("Processed")) {
	                            futures.add(executorService.submit(() -> {
	                                try {
	                                    String filemtime = oListItem.getAttrs().getAtimeString();
	                                    SimpleDateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
	                                    Date date = new Date();
	                                    String systime = formatter.format(date);
	                                    Date d1 = formatter.parse(filemtime);
	                                    Date d2 = formatter.parse(systime);
	                                    long diff = d2.getTime() - d1.getTime();
	                                    long diffMinutes = diff / (60 * 1000);
	                                    
	                                    if (diffMinutes > 1) {
	                                        String destinationFolder1 = Data.get("destination");
	                                        String destinationFolder = "D:/Drive/" + destinationFolder1;
	                                        File pathName = new File(destinationFolder);
	                                        if (!pathName.exists()) {
	                                            pathName.mkdirs();
	                                        }

	                                        channelSftp[0].lcd(destinationFolder);
	                                        channelSftp[0].get(oListItem.getFilename(), oListItem.getFilename());
	                                        logger.debug("Downloaded file: " + oListItem.getFilename());

	                                        channelSftp[0].rm(oListItem.getFilename());
	                                        DatabaseMaster.FileInformation(bankcode, oListItem.getFilename(), value, conn1);
	                                    }
	                                } catch (Exception e) {
	                                    logger.error("Error downloading file: " + oListItem.getFilename(), e);
	                                }
	                            }));
	                        }
	                    }

	                    // Wait for all download tasks to complete
	                    for (Future<?> future : futures) {
	                        try {
	                            future.get();
	                        } catch (Exception e) {
	                            logger.error("Error waiting for download task to complete", e);
	                        }
	                    }
	                } else {
	                    // Handle upload
	                	System.out.println("Its going in"+ value);
	                    File folder = new File(Data.get("source"));
	                    FileFilter fileFilter = f1 -> !f1.isDirectory() && f1.getName().startsWith(bankcode) && (f1.getName().endsWith(".xml") || f1.getName().contains(".XML"));
	                    File[] files = folder.listFiles(fileFilter);

	                    List<Future<?>> uploadFutures = new ArrayList<>();
	                    
	                    if (files != null) {
	                        for (File f : files) {
	                            uploadFutures.add(executorService.submit(() -> {
	                                try {
	                                    String source2 = f.toString();
	                                    FileInputStream fis = new FileInputStream(new File(source2));
	                                    String destinationpath = Data.get("destination");
	                                    channelSftp[0].cd(home + destinationpath);
	                                    channelSftp[0].put(fis, f.getName());
	                                    fis.close();

	                                    DatabaseMaster.FileInformation(bankcode, f.getName(), value, conn1);

	                                    String destinationArchivePath = "D:/Drive/" + bankcode + Data.get("destination_archieve");
	                                    File dir = new File(destinationArchivePath);
	                                    if (!dir.exists()) {
	                                        dir.mkdirs();
	                                    }

	                                    if (f.length() > 0) {
	                                        boolean success = f.renameTo(new File(dir, f.getName()));
	                                        if (!success) {
	                                            String shCommand = "mv " + f.toString() + " " + dir + "/" + f.getName();
	                                            Runtime.getRuntime().exec(shCommand);
	                                        }
	                                    } else {
	                                        logger.info("File size is zero: " + f.getName());
	                                    }
	                                } catch (Exception e) {
	                                    logger.error("Error uploading file: " + f.getName(), e);
	                                }
	                            }));
	                        }

	                        // Wait for all upload tasks to complete
	                        for (Future<?> future : uploadFutures) {
	                            try {
	                                future.get();
	                            } catch (Exception e) {
	                                logger.error("Error waiting for upload task to complete", e);
	                            }
	                        }
	                    }
	                }
	            }
	        }
	    } catch (Exception e) {
	        logger.error("Exception occurs while transferring file", e);
	    } finally {
	        try {
	            if (session != null) session.disconnect();
	            if (channel != null) channel.disconnect();
	            if (channelSftp[0] != null) {
	                channelSftp[0].disconnect();
	                channelSftp[0].exit();
	            }
	            if (filelist != null) filelist.clear();
	        } catch (Exception e) {
	            logger.error("Exception while closing connection", e);
	        }
	        executorService.shutdown();
	    }
	}

	public LinkedHashMap<String, String> server_details1(String bankcode,Connection connection3)
	{
//		Connection connection3 =  null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet=null;
		LinkedHashMap<String, String>serverInfo=new LinkedHashMap<String, String>();
		try
		{
			 if (connection3==null ||connection3.isClosed())
				{
			connection3=DatabaseMaster.getConnection(bankcode);
				}
			String query="select PFMSFTPIP,PFMSFTPUSER,PFMSFTPPWD from all_banks where bankcode='"+bankcode+"'";

		//	String query="select CPSMSFTPIP,CPSMSFTPUSR,CPSMSFTPPWD from all_banks where cpsmsbankcode='"+bankcode+"'";
			System.out.println(query);
			logger.info("server_details sQL="+query);
			preparedStatement = connection3.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();
			if(resultSet.next())
			{
				serverInfo.put("ip", resultSet.getString("PFMSFTPIP"));
				serverInfo.put("username", resultSet.getString("PFMSFTPUSER"));
				serverInfo.put("pfms_password", resultSet.getString("PFMSFTPPWD"));
			}



		}catch(Exception e)
		{

		}finally
		{
			try
			{
				if(preparedStatement!=null)
					preparedStatement.close();
				if(resultSet!=null)
					resultSet.close();
				
			}catch(Exception e)
			{

			}

		}
		return serverInfo;

	}

	public  LinkedHashMap<Integer, HashMap> fetchcommonData(String bankcode, Connection connection1)
	{
//		Connection connection1=null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet=null;
		LinkedHashMap<Integer, HashMap>map=new LinkedHashMap<Integer, HashMap>();
		try
		{ if (connection1==null ||connection1.isClosed())
		{
			connection1=DatabaseMaster.getConnection(bankcode);
		}
			int count=0;
			String query="select PFMS_SFTP_PATH,CEDGE_SFTP_PATH,RTRIM(TRIM(PFMS_SFTP_PATH),'/')||'Archive/' AS PFMS_SFTP_PATH_ARCH,FLOW from APP_SFTP_PATH1";
			logger.info("fetchcommonData SQL:-"+query);
			preparedStatement = connection1.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();


			LinkedHashMap<String, String>map1=null;
			while(resultSet.next())
			{
				count=count+1;
				map1=new LinkedHashMap<String, String>();
				map1.put("source", resultSet.getString("PFMS_SFTP_PATH"));
//				map1.put("source_archive", resultSet.getString("CEDGE_SFTP_PATH_ARCH"));
				map1.put("destination", resultSet.getString("CEDGE_SFTP_PATH"));
				map1.put("destination_archieve", resultSet.getString("PFMS_SFTP_PATH_ARCH"));
				map1.put("FLOW", resultSet.getString("FLOW"));

				map.put(count, map1);	 
			}

		}catch(Exception e)
		{
			e.printStackTrace();
		}finally
		{
			try
			{
				if(preparedStatement!=null)
					preparedStatement.close();
				if(resultSet!=null)
					resultSet.close();

			}catch(Exception e)
			{

			}
		}
		return map;

	}
}
