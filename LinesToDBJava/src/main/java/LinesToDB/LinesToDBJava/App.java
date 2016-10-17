package LinesToDB.LinesToDBJava;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
		Logger logger = LoggerFactory.getLogger(LinesToDB.class);
	    LinesToDB.setLogger(logger);
	    LinesToDB ltt = new LinesToDB();
	    /* Initialize the line parser with the regex groups per type of line 
	     * The format of the parse constructor can be viewed constructor of LinesToDB.parser*/
	    //1. A line from the netscaler config file  (refers to content server bindings)
	    ltt.addParseLine(new LinesToDB.parser("csLine",
				"bind cs vserver ([^\\s]*)\\s-policyName\\s([^\\s]*)\\s"
						//+ "-targetLBVserver\\s([^\\s]*)\\s"
						+ "(?:-targetLBVserver\\s([^\\s]*)\\s)?"
						+ "-priority\\s([^\\s]*)(?:\\s.*|$)","ns_csBehind"
				,"csVserver,policy,targetLb,priority",null
				));
	    //2. A line from AMQ configs
	    ltt.addParseLine(new LinesToDB.parser("qLine",
				"\\s*QUEUE\\(([^\\)\\(]+)\\)\\s*TYPE\\(([^\\)\\(]+)\\).*"
	    		,"QM_queues"
				,"QName,QType","QMName=ETSITQM2,env=SysInt"
				));
	    //3. Another type of line from AMQ configs
	    ltt.addParseLine(new LinesToDB.parser("channelLine",
				"\\s*CHANNEL\\(([^\\)\\(]+)\\)\\s*CHLTYPE\\(([^\\)\\(]+)\\).*"
	    		,"QM_queues"
				,"QName,QType","QMName=CH_DITQM2,env=DevInt"
				));
		
	    int parseFile = 0;
	    switch (parseFile) {
	    case 1:
			//String fileNM = "c:\\junk\\sunburst.conf.ets.cfm";
			String fileNM = "c:\\junk\\C_DITQM3.txt";
			EntityManagerFactory emf = Persistence.createEntityManagerFactory("mysqlTables"); //( "sqliteTables");
			EntityManager em = emf.createEntityManager();
			//LinesToTable.truncateTables(em); //(This should be a parameter - to truncate or not)
			System.out.println(ltt.parseFile(em,fileNM,logger,null));
			//parseNetScaler("c:\\junk\\ns.conf");
			System.exit(0);
	    case 0:
		//Test with a list of strings
			ArrayList<String> linesToProcess = new ArrayList<String>();
			linesToProcess.add("   QUEUE(SECURITY.FROMSECMASTER.LQ)        TYPE(QLOCAL)");
			linesToProcess.add("command=SeqnCtrlSvr -Q SeqnCtrl2  -s Order_Ets_GetEventNxtSqn:GETNEXTSEQN -- -SUSDTDIT -LY -Utuxuser -Ptuxedo -DETS_OrderEventDB -OADP");
			linesToProcess.add("bind cs vserver us-cs.sit-443 -policyName cspol.lbsit1m7.us-spahw -priority 220");
			for (String lineToProcess:linesToProcess) {
			LinesToDB.logInfoOnLine(lineToProcess, ltt.parseLine(lineToProcess), logger);
			}
			System.exit(0);
	    };

	}
}
