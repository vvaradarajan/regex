package LinesToDB.LinesToDBJava;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.Predicate;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/** Rewriting this with dynamic input instead of enum:
 * Pass the parsing instruction Array to this.
 * Create a Map<String,Object> (should be static)
 * Create an object of itself and add to the map
 * Each objects has a method that returns another of the same type or null (its own factory, with pseudo constructor)
 * Use the methods of the returned object as normal.
 * @author vvaradar
 *
 */
public class LinesToDB {
	public static class parser {
		public parser(String lineType, String regex, String tblNM,
				String csvFldNMs, String staticFlds) {
			super();
			this.lineType = lineType;
			this.regex = regex;
			this.tblNM = tblNM;
			this.csvFldNMs = csvFldNMs;
			this.staticFlds = staticFlds;
		}
		String lineType;
		String regex;
		String tblNM;
		String csvFldNMs;
		String staticFlds;
		DateFormat df; 
	};
	//'global' are defined as static
	static Map<String,LinesToDB> lineTypes = new HashMap<String,LinesToDB>();
	static Map<String,Integer> lineCounts=new HashMap<String,Integer>();
	//2015-04-17 18:57:39
	static SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
	static Date dl;
	parser lp;
    Integer lineCounter;
    private Pattern pattern;   // regex pattern
    private String tblNM;
    public static class fieldStruct {
    	String name;
    	String rule;
    	fieldStruct(String name, String rule) {
    		this.name=name; this.rule=rule;
    	};
    	static  fieldStruct[] getFlds(String csvFldDesc) {
    		String[] fldStrings = csvFldDesc.split(",");
    		fieldStruct[] flds = new fieldStruct[fldStrings.length];
    		String name;
    		String rule;
    		int i=0;
    		for (String s:fldStrings) {
    			String[] st=s.split(":");
    			name = st[0];
    			if (st.length>1) rule=st[1]; else rule=null;
    			flds[i++]=new fieldStruct(name,rule);
    		}
    		return flds;
    	}
    }
    private fieldStruct[] fields;
    private int noOfPredefinedFlds;
    private String[] predefinedFieldNMs;
    private String[] predefinedFieldValues;
    private static Logger logger;
	LinesToDB() {try {
		LinesToDB.dl= df.parse("2015-01-01 00:00:00");
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		System.exit(1);
	}}
	public void addParseLine(LinesToDB.parser parserline) {
		LinesToDB lt = new LinesToDB();
		lt.lp=parserline;
		lineTypes.put(lt.lp.lineType,lt);
		lt.lineCounter = new Integer(0);
		lineCounts.put(parserline.lineType, lt.lineCounter);
        lt.pattern = Pattern.compile(lt.lp.regex);
        lt.fields = fieldStruct.getFlds(lt.lp.csvFldNMs);
        lt.tblNM=lt.lp.tblNM;
        lt.noOfPredefinedFlds=0;
        if (lt.lp.staticFlds !=null) {
            String [] predefinedFldsValues = lt.lp.staticFlds.split(",");
            lt.noOfPredefinedFlds = predefinedFldsValues.length;
            lt.predefinedFieldNMs = new String[lt.noOfPredefinedFlds];
            lt.predefinedFieldValues = new String[lt.noOfPredefinedFlds];
            int i=0;
            for (String s:predefinedFldsValues) {
            	String[] s1 = s.split("=");
            	lt.predefinedFieldNMs[i] = new String(s1[0]);
            	lt.predefinedFieldValues[i]= s1[1];
            	i++;
            }
        }

	}
/*design of the generic file line by line decomposer
1. Create a enumeration of the types of lines in file
	a. Pattern to recognize and parse the line.
	b. The associated table to that line.
	c. Functions to parse and to save to that table.
	d. Function that indicates that the line is recognized and parsed.
	e. The main routine will pass the line through the enumeration until it is parsed.
*/
    private String toString(ArrayList<String> a) {
    	StringBuffer s = new StringBuffer();
    	int len = (a.size()-1>fields.length?fields.length:a.size()-1);
    	for (int i =0;i<len;i++) {
    		s.append(fields[i].name + "=" + a.get(i+1));  //0th element is the line itself - fields start at 1
    	}
    	//if (s.length()>0) System.out.println(s.toString());
    	return s.toString();
    }
    private String getInsertStmt(ArrayList<String> fldValues) {
    	StringBuffer s = new StringBuffer();
    	StringBuffer s1 = new StringBuffer();
    	int len = (fldValues.size()-1>fields.length?fields.length:fldValues.size()-1);
    	if (len > 0) s1.append("Insert into " + tblNM +"(");
    	for (int i =0;i<len;i++) {
    		s1.append(fields[i].name+',');
    		String fldValueString=fldValues.get(i+1);//0th element is the line itself - fields start at 1
    		if (fields[i].rule==null) s.append("'"+fldValueString+"',");
    		else if (fields[i].rule.equals("date")) {
    			//2015-04-17 18:57:39
    			Date dt;
				try {
					dt = df.parse(fldValueString);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					dt=null;
				}
    			if (dt.before(dl)) return null;
    			fldValueString = "STR_TO_DATE('"+fldValueString+"','%Y-%m-%d %H:%i:%s')";
    			s.append(fldValueString+",");
    			
    		}
    		  
    	}
    	// add any predefinedFields
    	for (int i=0;i<noOfPredefinedFlds;i++){ 
    		s1.append(predefinedFieldNMs[i]+',');
    		s.append("'"+predefinedFieldValues[i]+"',");
    	}
    	if (s.length()>0){
    		s.deleteCharAt(s.lastIndexOf(","));
    		s.append(")");
    		s1.deleteCharAt(s1.lastIndexOf(","));
    		s1.append(") values ("+s.toString());
    	}
    	return s1.toString();
    }
    private String isMatch(String line) {
    	String insertStmt=null;
    	Matcher m = pattern.matcher(line);
    	ArrayList<String> a = new ArrayList<String>();
    	if (m.find()) {	
    		//System.out.println("GroupCount="+ m.groupCount());
    		//for (int i=0;i<=m.groupCount();i++) System.out.println("Group="+ m.group(i));
			for (int i=0;i<=m.groupCount();i++) 
				if (m.group(i) != null) a.add(m.group(i) != null?m.group(i).replace("'","''"):null);
			insertStmt=getInsertStmt(a);
			//System.out.println(insertStmt);
			if (insertStmt == null) {logger.warn("No insert Stmt for : " + line);};
    	}
    	//toString(a);
    	return insertStmt;
    }
	public String parseLine (String line) {
		String insertStmt;
		for (LinesToDB  p : LinesToDB.lineTypes.values()) {
		 if ((insertStmt=p.isMatch(line))!=null){
			 //add match to counter
			 Integer lc =LinesToDB.lineCounts.get(p.lp.lineType);
			 lc++;
			 return insertStmt;
		 }
		}
		logger.warn("No Match: " + line);
		return null;
	}
	
	public static void setLogger(Logger logger) {
		LinesToDB.logger = logger;
	}
	public static void truncateTables(EntityManager em) {
		Set<String> tableNMs = new HashSet<String>();
		for (LinesToDB p : LinesToDB.lineTypes.values()) tableNMs.add(p.tblNM);
		for (String tblNM : tableNMs) {
			 em.getTransaction().begin();
			 String tstmt = "Truncate table "+tblNM;
			 logger.info("Executiing: "+ tstmt);
			 em.createNativeQuery(tstmt).executeUpdate();
			 em.getTransaction().commit();
			 }
	}

	public boolean filter(Date dt,Predicate<Date> tester) {
		if (tester.test(dt)) return true;
		else return false;
	}
	public String parseFile (EntityManager em, String fileNM,Logger logger,Predicate<Date> filter) throws IOException {
		LinesToDB.logger=logger;
		BufferedReader nsf = new BufferedReader(new FileReader(fileNM));
		String fline;
		int noOfLines=0;
		int maxLines=15000;
		int commitBlock=100;
		int noOfinsertLines=0;
		em.getTransaction().begin();
		while ((fline=nsf.readLine())!=null && noOfLines < maxLines) {
			String insertStmt = parseLine(fline);
			if (insertStmt != null){
				//System.out.println(insertStmt);
				try {
					noOfinsertLines ++;
					em.createNativeQuery(insertStmt).executeUpdate();
					if (noOfinsertLines>=commitBlock) {
						noOfinsertLines=0;
						System.out.print("."); //Start file processing..and print dots every commitBlock xxxxxxxxrecs. dont use logger as it uses println
						em.getTransaction().commit();
						em.getTransaction().begin();
					}
				} catch(Exception E) {
					E.printStackTrace();
					logger.error("Error Occured in : " + insertStmt,E);
					em.getTransaction().rollback();
					em.getTransaction().begin();
				}
			};
			noOfLines++;
		}
		em.getTransaction().commit();
		nsf.close();
		StringBuffer summarySB = new StringBuffer("NoOfLinesInFile= " + noOfLines+"; ");
		for (Map.Entry<String, Integer> entry : LinesToDB.lineCounts.entrySet()) { 
			summarySB.append( entry.getKey()+"= "+entry.getValue()+ "; ");
		}
		return summarySB.toString();
	}
	@Override
	public String toString() {
		StringBuffer s = new StringBuffer();
		for (LinesToDB  p : LinesToDB.lineTypes.values()) {
			 s.append(p.lp.regex);
			 s.append('\n');
		}
		return s.toString();
	}
	/* Some common patterns*/
	public static void rpmLines(String fileNM,EntityManager em,Logger logger) throws IOException {
	    LinesToDB.setLogger(logger);
	    LinesToDB ltt = new LinesToDB();
	    //-rwxr-xr-x  1 etbuild  etbuild  103001000 Feb 27 18:54 spa_trading-dit20160227.2-0.i386.rpm
	    ltt.addParseLine(new LinesToDB.parser("rpmLine",
				"([^\\s]+)\\s+(?:[^\\s]+)\\s+([^\\s]+)\\s+([^\\s]+)\\s+([^\\s]+)\\s+([0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.[0-9]+\\s-[0-9]+)\\s+([^\\s]+)(?:[\\s]+|$).*","em_rpms"
				,"permissionString,owner,unixGroup,size,modifiedDate:date,rpmNM,pkgNM",null
				));
	    logger.info(ltt.toString());
	    int parseFile = 1;
	    switch (parseFile) {
	    case 1:
			LinesToDB.truncateTables(em);
			System.out.println(ltt.parseFile(em,fileNM,logger,null));
			//parseNetScaler("c:\\junk\\ns.conf");
			System.exit(0);
	    case 0:
		//Test with string
			String lineToProcess;
			lineToProcess = "-rwxr-xr-x  1 etbuild  etbuild  103001000 Feb 27 18:54 spa_trading-dit20160227.2-0.i386.rpm";
			//lineToProcess="bind cs vserver us-cs.sit-443 -policyName cspol.lbsit1m7.us-spahw -priority 220";
			System.out.println(lineToProcess+"\n"+ltt.parseLine(lineToProcess));
			lineToProcess="bind cs vserver us-cs.sit-443 -policyName cspol.lbsit1m7.us-spahw -priority 220";
			System.out.println(lineToProcess+"\n"+ltt.parseLine(lineToProcess));
			System.exit(0);
	    };	
	}
		
	public static void main(String[] args) throws IOException, ParseException {
	//

		Logger logger = LoggerFactory.getLogger(LinesToDB.class);
	    LinesToDB.setLogger(logger);
	    LinesToDB ltt = new LinesToDB();
	    /*
	    ltt.addParseLine(new LinesToTable.parser("dbLine",
				"command=([^\\s]*)\\s.*(?:-D)([^\\s]+).*","em_instanceDBList"
				,"dataSvr,logicalDbName","inst=TBD"
				));

	    ltt.addParseLine(new LinesToTable.parser("csLine",
				"bind cs vserver ([^\\s]*)\\s-policyName\\s([^\\s]*)\\s"
						//+ "-targetLBVserver\\s([^\\s]*)\\s"
						+ "(?:-targetLBVserver\\s([^\\s]*)\\s)?"
						+ "-priority\\s([^\\s]*)(?:\\s.*|$)","em_ns_csBehind"
				,"csVserver,policy,targetLb,priority",null
				));
				*/
	    /*ltt.addParseLine(new LinesToTable.parser("qLine",
				"\\s*QUEUE\\(([^\\)\\(]+)\\)\\s*TYPE\\(([^\\)\\(]+)\\).*"
	    		,"em_QM_queues"
				,"QName,QType","QMName=ETSITQM2,env=sit"
				));*/
	    ltt.addParseLine(new LinesToDB.parser("channelLine",
				"\\s*CHANNEL\\(([^\\)\\(]+)\\)\\s*CHLTYPE\\(([^\\)\\(]+)\\).*"
	    		,"em_QM_queues"
				,"QName,QType","QMName=CH_ETDITQM2,env=dit"
				));
	    logger.info(ltt.toString());
		
	    int parseFile = 0;
	    switch (parseFile) {
	    case 1:
			//String fileNM = "c:\\junk\\sunburst.conf.ets.cfm";
			String fileNM = "c:\\junk\\C_ETDITQM2.txt";
			EntityManagerFactory emf = Persistence.createEntityManagerFactory("mysqlTables"); //( "sqliteTables");
			EntityManager em = emf.createEntityManager();
			//LinesToTable.truncateTables(em); //(This should be a parameter - to truncate or not)
			System.out.println(ltt.parseFile(em,fileNM,logger,null));
			//parseNetScaler("c:\\junk\\ns.conf");
			System.exit(0);
	    case 0:
		//Test with string
			String lineToProcess;
			//lineToProcess = "command=SeqnCtrlSvr -Q SeqnCtrl2  -s Order_Ets_GetEventNxtSqn:GETNEXTSEQN -- -SUSDTDIT -LY -Utuxuser -Ptuxedo -DETS_OrderEventDB -OADP";
			//lineToProcess="bind cs vserver us-cs.sit-443 -policyName cspol.lbsit1m7.us-spahw -priority 220";
			lineToProcess = "   QUEUE(SECURITY.FROMSECMASTER.LQ)        TYPE(QLOCAL)";
			System.out.println(lineToProcess+"\n"+ltt.parseLine(lineToProcess));
			lineToProcess="bind cs vserver us-cs.sit-445 -policyName cspol.lbsit1m7.us-spahw -priority 220";
			System.out.println(lineToProcess+"\n"+ltt.parseLine(lineToProcess));
			System.exit(0);
	    };

	}
}