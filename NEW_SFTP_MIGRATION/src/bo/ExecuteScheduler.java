package bo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;
import utilities.GenericUtilities;

public class ExecuteScheduler {

    public static final Logger logger = Logger.getLogger(ExecuteScheduler.class);
    private static final int THREAD_POOL_SIZE = 10; // Number of threads in the pool

    public static void main(String[] args) {
        Timer timer = new Timer();    
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

                try {
                    Properties props = GenericUtilities.databaseProp;
                    String dataBaseServer1 = props.getProperty("DatabaseServerForAllApp");
                    String userName1 = props.getProperty("UsernameForAllApp");
                    String password1 = props.getProperty("PasswordForAllApp");
                    String Driver = props.getProperty("Driver");
                    
                    Class.forName(Driver);  
                    
                    // Get bank codes
                    String bankcode = getBankCodes(dataBaseServer1, userName1, password1);
                    String[] acarray = bankcode.split(",");
                    
                    for (final String bankcode1 : acarray) {
                        executorService.submit(() -> {
                            try (Connection con = DriverManager.getConnection(dataBaseServer1, userName1, password1)) {
                                new ExecuteScheduler().runQuick(bankcode1, con);
                            } catch (Exception e) {
                                logger.error("Error processing bank code " + bankcode1, e);
                            }
                        });
                    }
                    
                } catch (Exception e) {
                    logger.error("Exception occurred", e);
                } finally {
                    executorService.shutdown();
                }
            }
        }, 1000, 30000);
    }

    private static String getBankCodes(String dataBaseServer, String userName, String password) throws SQLException, ClassNotFoundException {
        StringBuilder bankcode = new StringBuilder();
        
        try (Connection con = DriverManager.getConnection(dataBaseServer, userName, password);
             Statement stmt1 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            
            String sql = "select distinct bankcode from all_banks1 where PSFTPFLMVFLG='Y'";
            ResultSet res1 = stmt1.executeQuery(sql);
            
            while (res1.next()) {
                bankcode.append(res1.getString("bankcode")).append(",");
            }
            
            if (res1 != null) res1.close();
        }
        
        return bankcode.toString();
    }

    public void runQuick(final String bankCode, Connection con) {
        logger.info("Start Time for Bank --" + bankCode);
        long startTime = System.nanoTime();
        logger.info("Start Time --" + new Date());
        
        try {
        	new MiddleWare().processFunction2(bankCode, con);
        } catch (Exception e) {
            logger.error("Error processing bank code " + bankCode, e);
        }
        
        long endTime = System.nanoTime();
        Date date1 = new Date();
        logger.info("End Time --" + date1);
        long totalTime = (endTime - startTime) / 1000000000;
        logger.info("Total Run time Taken  " + totalTime + " Seconds \n");
    }
}
