package com.ericsson.dm.transformation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

import com.ericsson.dm.init.InitializationStep;
import com.ericsson.dm.transformation.impl.Account;
import com.ericsson.dm.transformation.impl.DedicatedAccount;
import com.ericsson.dm.transformation.impl.Offer;
import com.ericsson.dm.transformation.impl.Subscriber;
import com.ericsson.dm.transformation.impl.UsageCounter;
import com.ericsson.dm.transformation.impl.UsageThreshold;

import com.ericsson.jibx.beans.SUBSCRIBER;
import com.ericsson.jibx.beans.SUBSCRIBER.EOCLIST.EOCINFO;
import com.ericsson.jibx.beans.SUBSCRIBER.POSTPAIDLIST.POSTPAIDINFO;
import com.ericsson.jibx.beans.SUBSCRIBER.PREPAIDLIST.PREPAIDINFO;
import com.ericsson.jibx.beans.SUBSCRIBER.SHAREDDATALIST.SHAREDDATAINFO;

public class UnMarshaller {

	private IBindingFactory bfactZainStaging;
	private IUnmarshallingContext uctxZainStaging;
	

	private String pathOfOutputFolder;
	private String pathOfLogFolder;
	final static Logger LOG = Logger.getLogger(UnMarshaller.class);
	private String pathtoApplicationContext;
	private int uniqueNumber;
	private List<String> accountListBuffer;
	private List<String> subscriberListBuffer;
	private List<String> offerListBuffer;
	private List<String> offerAttributeListBuffer;
	private List<String> pamListBuffer;
	private List<String> subscriberOfferListBuffer;
	private List<String> subscriberOfferAttributeListBuffer;
	private List<String> providerOfferListBuffer;
	private List<String> accumulatorBuffer;
	private List<String> ucBuffer;
	private List<String> utBuffer;
	private List<String> daBuffer, providerucBuffer, providerutBuffer;
	private  List<String> rejectAndLog;
	private  List<String> discardAndLog;
	private  List<String> onlyLog, notMigratedLog;
	public Map<String, Set<String>> mapofValidUcId;
	public Map<String, Set<String>> mapofValidProviderUcId;

	// private List<String> smallFiles;

	private static final String DACOLUMNS = "ID_1,BALANCE_1,START_DATE_1,EXPIRY_DATE_1,PAM_SERVICE_ID_1,PRODUCT_ID_1,ID_2,BALANCE_2,START_DATE_2,EXPIRY_DATE_2,PAM_SERVICE_ID_2,PRODUCT_ID_2,ID_3,BALANCE_3,START_DATE_3,EXPIRY_DATE_3,PAM_SERVICE_ID_3,PRODUCT_ID_3,ID_4,BALANCE_4,START_DATE_4,EXPIRY_DATE_4,PAM_SERVICE_ID_4,PRODUCT_ID_4,ID_5,BALANCE_5,START_DATE_5,EXPIRY_DATE_5,PAM_SERVICE_ID_5,PRODUCT_ID_5,ID_6,BALANCE_6,START_DATE_6,EXPIRY_DATE_6,PAM_SERVICE_ID_6,PRODUCT_ID_6,ID_7,BALANCE_7,START_DATE_7,EXPIRY_DATE_7,PAM_SERVICE_ID_7,PRODUCT_ID_7,ID_8,BALANCE_8,START_DATE_8,EXPIRY_DATE_8,PAM_SERVICE_ID_8,PRODUCT_ID_8,ID_9,BALANCE_9,START_DATE_9,EXPIRY_DATE_9,PAM_SERVICE_ID_9,PRODUCT_ID_9,ID_10,BALANCE_10,START_DATE_10,EXPIRY_DATE_10,PAM_SERVICE_ID_10,PRODUCT_ID_10";

	protected static final Map<String, String> equipIdScMapping = new ConcurrentHashMap<String, String>(10000, 0.75f,
			20);

	public static void main(String args[]) throws IOException, JiBXException, Exception {
		test();
		;
	}

	public static void test() throws JiBXException, IOException, Exception {
		UnMarshaller obj = new UnMarshaller("C:/users/egjklol/Desktop/",
				"file:C:/Projects/Zainsaudi/SVN/CS/codebase_ra06/etl/tr/pentaho/dm_cs_2018_zain_saudi/dev/src/config/appCtx-Transformation.xml",
				"c:/Users/egjklol/Desktop/Output/");// ,"C:/Projects/Zainsaudi/SVN/CS/codebase_ra01/etl/tr/pentaho/dm_cs_2018_zain_saudi/dev/src/database");
		long starttime = Calendar.getInstance().getTimeInMillis();
		String line = "", xml = "";
		BufferedReader br = new BufferedReader(new FileReader("c:/Users/egjklol/Desktop/a.txt"));
		// "c:/Users/egjklol/Desktop/BigSubscriber.txt"
		// ));
		while ((line = br.readLine()) != null) {
			xml = line.trim();
			// try {
			obj.businessLogicImplementation(xml, "Static");
			/*
			 * SUBSCRIBER subs = null;//(SUBSCRIBER)
			 * uctxZainStaging.unmarshalDocument(new
			 * ByteArrayInputStream(xml.getBytes()), null); //subs =
			 * modifySubscriberObjectAccording2Delta(subs); //subs. for
			 * (PREPAIDINFO info : subs.getPREPAIDLIST1().getPREPAIDINFOList())
			 * { System.out.println(info.getEQUIPID() + " "+
			 * info.getEQUIPIDTYPE());
			 * if(info.getEQUIPID().equals("PREP_UNBAR")){
			 * info.setEQUIPID("XXXXX"); } }
			 * 
			 * for (PREPAIDINFO info :
			 * subs.getPREPAIDLIST1().getPREPAIDINFOList()) {
			 * System.out.println(info.getEQUIPID() + " "+
			 * info.getEQUIPIDTYPE());
			 * 
			 * }
			 */

			/*
			 * } catch (Exception e) { e.printStackTrace(); LOG.error(e); }
			 */
			;

		}
		// System.out.println(xml);
		obj.generateCsv();
		obj.clearBuffer();
		br.close();

		long endtime = Calendar.getInstance().getTimeInMillis() - starttime;
		System.out.println("Before generating csv: " + endtime);
		endtime = Calendar.getInstance().getTimeInMillis() - starttime;
		System.out.println(endtime);
	}

	public UnMarshaller(String pathOfOutputFolder, final String pathtoApplicationContext, String pathOfLogFolder) {
		this.pathOfOutputFolder = pathOfOutputFolder;
		this.pathOfLogFolder = "/" + pathOfLogFolder;

		this.pathtoApplicationContext = pathtoApplicationContext;
		accountListBuffer = new ArrayList<>();
		subscriberListBuffer = new ArrayList<>();
		offerListBuffer = new ArrayList<>();
		offerAttributeListBuffer = new ArrayList<>();
		pamListBuffer = new ArrayList<>();
		subscriberOfferListBuffer = new ArrayList<>();
		subscriberOfferAttributeListBuffer = new ArrayList<>();
		providerOfferListBuffer = new ArrayList<>();
		ucBuffer = new ArrayList<>();
		utBuffer = new ArrayList<>();
		daBuffer = new ArrayList<>();
		providerucBuffer = new ArrayList<>();
		providerutBuffer = new ArrayList<>();
		accountListBuffer = new ArrayList<>();
		accumulatorBuffer= new ArrayList<>();
		rejectAndLog = Collections.synchronizedList(new ArrayList<String>());
		discardAndLog = Collections.synchronizedList(new ArrayList<String>());
		onlyLog = Collections.synchronizedList(new ArrayList<String>());
		notMigratedLog = Collections.synchronizedList(new ArrayList<String>());
				try {

			bfactZainStaging = BindingDirectory.getFactory(SUBSCRIBER.class);
			uctxZainStaging = bfactZainStaging.createUnmarshallingContext();

		

			/*
			 * InitializationStep obj = new
			 * InitializationStep(this.pathtoApplicationContext, "C:/PA19/",
			 * "JD1SDP01",
			 * "C:/Projects/Zainsaudi/SVN/CS/codebase_ra06/etl/tr/pentaho/dm_cs_2018_zain_saudi/dev/src/config",
			 * "C:/Projects/Zainsaudi/SVN/CS/codebase_ra06/etl/tr/pentaho/dm_cs_2018_zain_saudi/dev/src/data",
			 * "FULL_TRANSFORMATION");
			 */

			// Generate random integers in range 0 to 999999
			uniqueNumber = InitializationStep.rand.nextInt(1000000);

		} catch (JiBXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("JIBX Exception ", e); 
		}

	}

	public void businessLogicImplementation(String xml, String delta) throws JiBXException, IOException {
		SUBSCRIBER subs;

		// InitializationStep obj = new
		// InitializationStep(this.pathtoApplicationContext,"","sdp01");
		try {
			// long starttime = Calendar.getInstance().getTimeInMillis();
			if (xml.getBytes().length > 10485760) {

				subs = (SUBSCRIBER) uctxZainStaging.unmarshalDocument(new ByteArrayInputStream(xml.getBytes()), null);

				// Make into smaller xmls
				LOG.warn("Cannot process the subscriber as it exceeds size of 10MB " + subs.getCUSTID());

				List<String> smallFiles = new ArrayList<>();
				smallFiles = generateSmallFiles(subs);
				File bigSubsFile = new File(this.pathOfOutputFolder + "BigSubscriber.txt");
				if (!bigSubsFile.exists()) {
					bigSubsFile.createNewFile();
				}
				FileUtils.writeLines(bigSubsFile, smallFiles, true);
				smallFiles.clear();
				return;
			}

			subs = (SUBSCRIBER) uctxZainStaging.unmarshalDocument(new ByteArrayInputStream(xml.getBytes()), null);
			Set<String> deltaMsisdn = new HashSet<String>();
			Set<String> validMsisdn = new HashSet<String>();
			
			
			
			
			
			Account account = new Account(subs,validMsisdn,rejectAndLog, discardAndLog, onlyLog);
			accountListBuffer.addAll(account.execute());

			Subscriber subscriber = new Subscriber(subs
					, validMsisdn, rejectAndLog, discardAndLog, onlyLog);
			subscriberListBuffer.addAll(subscriber.execute());
			
			Offer offer = new Offer(subs,validMsisdn, rejectAndLog, discardAndLog, onlyLog);
			Map<String, List<String>> result = offer.execute();

			UsageCounter uc = new UsageCounter(subs, 
					validMsisdn, rejectAndLog, discardAndLog, onlyLog, notMigratedLog);
			Map<String, List<String>> ucMap = uc.execute();
			UsageThreshold ut = new UsageThreshold(subs, 
					validMsisdn, rejectAndLog, discardAndLog, onlyLog, notMigratedLog);
			Map<String, List<String>> utMap = ut.execute();

			DedicatedAccount da = new DedicatedAccount(subs, 
					validMsisdn, rejectAndLog, discardAndLog, onlyLog, notMigratedLog);
			Map<String, Map<String, String>> daMap = da.execute();

			

			List<String> uclist = ucMap.get("UC");
			List<String> provideruclist = ucMap.get("PUC");
			List<String> utlist = utMap.get("UT");
			List<String> providerutlist = utMap.get("PUT");
			provideruclist = provideruclist == null ? new ArrayList<String>() : provideruclist;
			
			/*
			 * System.out.println("UC :"+ uclist); System.out.println("UT: "+
			 * utlist);
			 */

			//Commented as per the bug raised on 9/9/2018
			//Map<String, List<String>> updateUcUt = eleminateNotMatching(uclist, utlist);
			//uclist = updateUcUt.get("UC");
			//utlist = updateUcUt.get("UT");

			Set<String> msisdnList = daMap.keySet();
			List<String> daList = new ArrayList<>();

			for (String msisdn : msisdnList) {
				StringBuffer sb = new StringBuffer();
				String[] splittedColumns = DACOLUMNS.split(",");
				int columnCount = 1;
				Map<String, String> map = daMap.get(msisdn);
				if (map.containsKey("ID_1")) {
					sb.append(msisdn).append(",");
					sb.append(1).append(",");

					for (String column : splittedColumns) {
						if (columnCount <= splittedColumns.length - 1) {
							if (map.containsKey(column)) {
								sb.append(map.get(column)).append(",");
							} else {
								if (column.startsWith("ID") || column.startsWith("BALANCE")) {
									sb.append("0,");
								} else {
									sb.append("NULL,");
								}
							}
						} else {
							if (map.containsKey(column)) {
								sb.append(map.get(column));
							} else {

								sb.append("NULL");
							}
						}
						columnCount++;
					}
				}
				if (map.containsKey("ID_1")) {
					daList.add(sb.toString());
				}
				sb = null;
			}
			if (uclist != null) {
				ucBuffer.addAll(uclist);
			}
			// System.out.println(uclist);
			if (utlist != null) {
				utBuffer.addAll(utlist);
			}
			daBuffer.addAll(daList);
			if (provideruclist != null) {
				providerucBuffer.addAll(provideruclist);
			}
			if (providerutlist != null) {
				providerutBuffer.addAll(providerutlist);
			}

			// fulllist.add(accountList);
			// fulllist.add(subsList);
			validMsisdn.clear();
			validMsisdn = null;
			mapofValidUcId.clear();
			mapofValidProviderUcId.clear();
			//mapOfSdpDataFromDA.clear();
			// fulllist.add(accountList);
			// fulllist.add(subsList);
			// validMsisdn.clear();
			account = null;
			subscriber = null;
			subs = null;
			validMsisdn = null;
			// generateCsv(accountList,subsList,offerOnlyList,offerAttrOnlyList);

		} catch (JiBXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("JIBX Exception ", e);
			LOG.error(xml);
			// System.out.println(xml);
		}
		// return fulllist;
	}

	
	
	private Map<String, List<String>> eleminateNotMatching(List<String> uclist, List<String> utlist) {
		// TODO Auto-generated method stub
		Map<String, Map<String, String>> mapUC = new WeakHashMap<>();
		List<String> newUcList = new ArrayList<String>();
		List<String> newUtList = new ArrayList<String>();
		Map<String, List<String>> result = new WeakHashMap<>();
		if (uclist != null) {
			for (String ucdata : uclist) {
				String splittedData[] = ucdata.split(",");
				if (mapUC.containsKey(splittedData[0])) {
					Map<String, String> value = mapUC.get(splittedData[0]);
					value.put(splittedData[1], ucdata);
					mapUC.put(splittedData[0], value);
				} else {
					Map<String, String> value = new WeakHashMap<>();
					value.put(splittedData[1], ucdata);
					mapUC.put(splittedData[0], value);
				}
			}
		}
		// System.out.println(mapUC);
		if (utlist != null) {
			for (String utdata : utlist) {
				String splittedData[] = utdata.split(",");
				if (mapUC.containsKey(splittedData[0])) {
					if (mapUC.get(splittedData[0]).containsKey(splittedData[1])) {
						newUcList.add(mapUC.get(splittedData[0]).get(splittedData[1]));
						newUtList.add(utdata);
					}
				}
			}
		}
		// System.out.println("newUClist:" + newUcList);
		if (newUcList != null) {
			Set<String> hs = new HashSet<>();
			hs.addAll(newUcList);
			newUcList.clear();
			newUcList.addAll(hs);
		}

		if (newUtList != null) {
			Set<String> hs1 = new HashSet<>();
			hs1.addAll(newUtList);
			newUtList.clear();
			newUtList.addAll(hs1);
		}
		result.put("UC", newUcList);
		result.put("UT", newUtList);
		return result;

	}

	private SUBSCRIBER modifySubscriberObjectAccording2Delta(SUBSCRIBER subs, Set<String> deltaMsisdn) {
		// TODO Auto-generated method stub
		Set<String> uniqueMsisdn = new HashSet<>();
		// List<POSTPAIDINFO> listOfpp = new ArrayList<>();
		for (POSTPAIDINFO postpaidinfo : subs.getPOSTPAIDLIST1().getPOSTPAIDINFOList()) {
			String equipid = postpaidinfo.getEQUIPID();
			String msisdn = postpaidinfo.getMSISDN();
			String type = postpaidinfo.getEQUIPIDTYPE();
			if (InitializationStep.DELTA_CUSTOMER_MAP_EXCHANGE.containsKey(msisdn)) {
				Map<String, Set<String>> map = InitializationStep.DELTA_CUSTOMER_MAP_EXCHANGE.get(msisdn);
				if (map.containsKey(equipid)) {
					if (type.equals("M") || type.equals("T")) {
						rejectAndLog.add("TCR06: "
								+ "MSISDN present in STG_ADDON_OPTIN_OUT is trying to replace main equipment id hence rejected: "
								+ msisdn + "," + equipid);
					} else {
						Set<String> set = map.get(equipid);
						if (set.size() == 1) {
							for (String values : set) {
								String data[] = values.split(",");
								if (data != null && data.length > 2) {
									LOG.info("Change the equip id from " + msisdn + "," + equipid + "," + data[0]);
									postpaidinfo.setEQUIPID(data[0]);
									postpaidinfo.setVALIDFROM(data[1]);
									postpaidinfo.setVALIDTO(data[2]);
									deltaMsisdn.add(msisdn);
								}
							}
						}
					}
				} else {
					// Set<String> oldequipids = new HashSet<>();
					/*
					 * for (String values : set) {
					 * 
					 * String data[] = values.split(","); if (data != null &&
					 * data.length > 2) { LOG.info( "Change the equip id from "
					 * + msisdn + "," + equipid + "," + data[0]); POSTPAIDINFO p
					 * = new POSTPAIDINFO();
					 * postpaidinfo.setEQUIPID("XXXXXXXXXX");
					 * p.setEQUIPID(data[0]); p.setVALIDFROM(data[1]);
					 * p.setVALIDTO(data[2]); p.setCOACTDATE(data[1]);
					 * p.setCONTID(postpaidinfo.getCONTID());
					 * p.setMSISDN(postpaidinfo.getMSISDN());
					 * p.setSTATUS("ACTIVE"); p.setSUCCESSFLAG("0");
					 * p.setEQUIPIDTYPE(postpaidinfo.getEQUIPIDTYPE()); //
					 * subs.getPOSTPAIDLIST1().getPOSTPAIDINFOList().add(
					 * postpaidinfo); deltaMsisdn.add(msisdn); }
					 * 
					 * }
					 */
					rejectAndLog.add("TCR07: "
							+ "MSISDN present in STG_ADDON_OPTIN_OUT is trying to replace more than one equipment id "
							+ msisdn + "," + equipid);
				}
			} else if (InitializationStep.DELTA_CUSTOMER_MAP_DELETION.containsKey(msisdn)) {
				Map<String, String> map = InitializationStep.DELTA_CUSTOMER_MAP_DELETION.get(msisdn);
				if (map.containsKey(equipid)) {
					postpaidinfo.setEQUIPID("XXXXXXXXX");
					postpaidinfo.setVALIDFROM(null);
					postpaidinfo.setVALIDTO(null);
					deltaMsisdn.add(msisdn);
				}
			}
			uniqueMsisdn.add(msisdn);

		}
		for (PREPAIDINFO prepaidinfo : subs.getPREPAIDLIST1().getPREPAIDINFOList()) {
			String equipid = prepaidinfo.getEQUIPID().toUpperCase();
			String msisdn = prepaidinfo.getMSISDN();
			String type = prepaidinfo.getEQUIPIDTYPE();

			if (InitializationStep.DELTA_CUSTOMER_MAP_EXCHANGE.containsKey(msisdn)) {

				Map<String, Set<String>> map = InitializationStep.DELTA_CUSTOMER_MAP_EXCHANGE.get(msisdn);
				if (map.containsKey(equipid)) {
					// boolean flag = true;
					if (type.equals("M") || type.equals("T")) {
						rejectAndLog.add("TCR06: "
								+ "MSISDN present in STG_ADDON_OPTIN_OUT is trying to replace main equipment id hence rejected: "
								+ msisdn + "," + equipid);
						// flag = false;
					} else {
						Set<String> set = map.get(equipid);
						if (set.size() == 1) {
							for (String values : set) {
								// String values = map.get(equipid);
								String data[] = values.split(",");
								if (data != null && data.length > 2) {
									LOG.info("Change the equip id from " + msisdn + "," + equipid + "," + data[0] + ","
											+ data[1] + "," + data[2]);
									prepaidinfo.setEQUIPID(data[0]);
									prepaidinfo.setVALIDFROM(data[1]);
									prepaidinfo.setVALIDTO(data[2]);
									deltaMsisdn.add(msisdn);
									// bfactZainStaging.createMarshallingContext().
									// LOG.info(message);
								}
							}
						}
					}
				} else {/*
						 * for (String values : set) { // String values =
						 * map.get(equipid); String data[] = values.split(",");
						 * if (data != null && data.length > 2) { LOG.info(
						 * "Change the equip id from " + msisdn + "," + equipid
						 * + "," + data[0]); PREPAIDINFO p = new PREPAIDINFO();
						 * prepaidinfo.setEQUIPID("XXXXXXXX");
						 * prepaidinfo.setVALIDFROM(data[1]);
						 * prepaidinfo.setVALIDTO(data[2]); //
						 * subs.getPREPAIDLIST1().getPREPAIDINFOList().add(p );
						 * deltaMsisdn.add(msisdn); }
						 * 
						 * }
						 */
					rejectAndLog.add("TCR07: "
							+ "MSISDN present in STG_ADDON_OPTIN_OUT is trying to replace more than one equipment id "
							+ msisdn + "," + equipid);
				}
			} else if (InitializationStep.DELTA_CUSTOMER_MAP_DELETION.containsKey(msisdn)) {
				Map<String, String> map = InitializationStep.DELTA_CUSTOMER_MAP_DELETION.get(msisdn);
				if (map.containsKey(equipid)) {
					prepaidinfo.setEQUIPID("XXXXXXXXX");
					prepaidinfo.setVALIDFROM(null);
					prepaidinfo.setVALIDTO(null);
					deltaMsisdn.add(msisdn);
				}
			}
			// }
		}
		// LOG.info(subs);
		return subs;
	}

	private List<String> generateSmallFiles(SUBSCRIBER subs) {
		Map<String, String> mapOfXmls = new HashMap<>();
		Map<String, String> mapOfEocXmls = new HashMap<>();
		List<String> list = new ArrayList<>();
		String custId = subs.getCUSTID();
		String lang = subs.getLANG();
		String billcycle = subs.getBILLCYCLE();
		for (PREPAIDINFO info : subs.getPREPAIDLIST1().getPREPAIDINFOList()) {
			if (mapOfXmls.containsKey(info.getMSISDN())) {
				String smallxml = mapOfXmls.get(info.getMSISDN());
				if (smallxml != null) {
					StringBuffer sb = new StringBuffer(smallxml);
					sb.append("<PREPAID_INFO>").append("<CONT_ID>").append(info.getCONTID()).append("</CONT_ID>");
					sb.append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
					sb.append("<CO_ACT_DATE>").append(info.getCOACTDATE()).append("</CO_ACT_DATE>");
					sb.append("<EQUIP_ID>").append(info.getEQUIPID()).append("</EQUIP_ID>");
					sb.append("<EQUIPIDTYPE>").append(info.getEQUIPIDTYPE()).append("</EQUIPIDTYPE>");
					sb.append("<STATUS>").append(info.getSTATUS()).append("</STATUS>");
					sb.append("<VALID_FROM>").append(info.getVALIDFROM()).append("</VALID_FROM>");
					sb.append("<VALID_TO>").append(info.getVALIDTO()).append("</VALID_TO>");
					sb.append("<SUCCESS_FLAG>").append(info.getSUCCESSFLAG()).append("</SUCCESS_FLAG>")

							.append("</PREPAID_INFO>");
					mapOfXmls.put(info.getMSISDN(), sb.toString());
				}
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append("<PREPAID_INFO>").append("<CONT_ID>").append(info.getCONTID()).append("</CONT_ID>");
				sb.append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
				sb.append("<CO_ACT_DATE>").append(info.getCOACTDATE()).append("</CO_ACT_DATE>");
				sb.append("<EQUIP_ID>").append(info.getEQUIPID()).append("</EQUIP_ID>");
				sb.append("<EQUIPIDTYPE>").append(info.getEQUIPIDTYPE()).append("</EQUIPIDTYPE>");
				sb.append("<STATUS>").append(info.getSTATUS()).append("</STATUS>");
				sb.append("<VALID_FROM>").append(info.getVALIDFROM()).append("</VALID_FROM>");
				sb.append("<VALID_TO>").append(info.getVALIDTO()).append("</VALID_TO>");
				sb.append("<SUCCESS_FLAG>").append(info.getSUCCESSFLAG()).append("</SUCCESS_FLAG>")
						.append("</PREPAID_INFO>");
				mapOfXmls.put(info.getMSISDN(), sb.toString());
			}
		}

		for (POSTPAIDINFO info : subs.getPOSTPAIDLIST1().getPOSTPAIDINFOList()) {
			// System.out.println(info.getEQUIPID() + " "+
			// info.getEQUIPIDTYPE());
			if (mapOfXmls.containsKey(info.getMSISDN())) {
				String smallxml = mapOfXmls.get(info.getMSISDN());
				if (smallxml != null) {
					StringBuffer sb = new StringBuffer(smallxml);
					sb.append("<POSTPAID_INFO>").append("<CONT_ID>").append(info.getCONTID()).append("</CONT_ID>");
					sb.append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
					sb.append("<CO_ACT_DATE>").append(info.getCOACTDATE()).append("</CO_ACT_DATE>");
					sb.append("<EQUIP_ID>").append(info.getEQUIPID()).append("</EQUIP_ID>");
					sb.append("<EQUIPIDTYPE>").append(info.getEQUIPIDTYPE()).append("</EQUIPIDTYPE>");
					sb.append("<STATUS>").append(info.getSTATUS()).append("</STATUS>");
					sb.append("<VALID_FROM>").append(info.getVALIDFROM()).append("</VALID_FROM>");
					sb.append("<VALID_TO>").append(info.getVALIDTO()).append("</VALID_TO>");
					sb.append("<SUCCESS_FLAG>").append(info.getSUCCESSFLAG()).append("</SUCCESS_FLAG>")
							.append("</POSTPAID_INFO>");

					mapOfXmls.put(info.getMSISDN(), sb.toString());
				}
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append("<POSTPAID_INFO>").append("<CONT_ID>").append(info.getCONTID()).append("</CONT_ID>");
				sb.append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
				sb.append("<CO_ACT_DATE>").append(info.getCOACTDATE()).append("</CO_ACT_DATE>");
				sb.append("<EQUIP_ID>").append(info.getEQUIPID()).append("</EQUIP_ID>");
				sb.append("<EQUIPIDTYPE>").append(info.getEQUIPIDTYPE()).append("</EQUIPIDTYPE>");
				sb.append("<STATUS>").append(info.getSTATUS()).append("</STATUS>");
				sb.append("<VALID_FROM>").append(info.getVALIDFROM()).append("</VALID_FROM>");
				sb.append("<VALID_TO>").append(info.getVALIDTO()).append("</VALID_TO>");
				sb.append("<SUCCESS_FLAG>").append(info.getSUCCESSFLAG()).append("</SUCCESS_FLAG>")
						.append("</POSTPAID_INFO>");
				mapOfXmls.put(info.getMSISDN(), sb.toString());

			}
		}
		if (subs.getEOCLIST1() != null && subs.getEOCLIST1().getEOCINFOList() != null) {
			for (EOCINFO info : subs.getEOCLIST1().getEOCINFOList()) {
				String msisdn = info.getMSISDN();
				if (msisdn != null) {
					msisdn = info.getMSISDN().substring(3, msisdn.length());
				}
				if (mapOfEocXmls.containsKey(msisdn)) {
					String smallxml = mapOfEocXmls.get(msisdn);
					if (smallxml != null) {
						StringBuffer sb = new StringBuffer(smallxml);
						sb.append("<EOC_INFO>").append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
						sb.append("<ITEMID>").append(info.getITEMID()).append("</ITEMID>");
						sb.append("<ITEMCODE>").append(info.getITEMCODE()).append("</ITEMCODE>");
						sb.append("</EOC_INFO>");
						mapOfEocXmls.put(msisdn, sb.toString());
					}
				} else {
					StringBuffer sb = new StringBuffer();
					sb.append("<EOC_INFO>").append("<MSISDN>").append(info.getMSISDN()).append("</MSISDN>");
					sb.append("<ITEMID>").append(info.getITEMID()).append("</ITEMID>");
					sb.append("<ITEMCODE>").append(info.getITEMCODE()).append("</ITEMCODE>");
					sb.append("</EOC_INFO>");
					mapOfEocXmls.put(msisdn, sb.toString());

				}
			}

		}
		Set<String> msisdnSet = mapOfXmls.keySet();
		for (String msisdn : msisdnSet) {
			String smallxml = mapOfXmls.get(msisdn);
			if (smallxml.startsWith("POSTPAID_INFO")) {
				StringBuffer sb = new StringBuffer();
				sb.append("<SUBSCRIBER>").append("<CUST_ID>").append(custId).append("</CUST_ID>").append("<LANG>")
						.append(lang).append("</LANG>").append("<BILL_CYCLE>").append(billcycle)
						.append("</BILL_CYCLE>");
				;
				sb.append("<POSTPAID_LIST>").append(smallxml).append("</POSTPAID_LIST>");
				sb.append("<PREPAID_LIST>").append("</PREPAID_LIST>");
				if (mapOfEocXmls.get(msisdn) != null)
					sb.append("<EOC_LIST>").append(mapOfEocXmls.get(msisdn)).append("</EOC_LIST>");
				else
					sb.append("<EOC_LIST>").append("</EOC_LIST>");
				sb.append("</SUBSCRIBER>");
				// System.out.println(sb.toString());
				list.add(sb.toString());
				mapOfXmls.put(msisdn, sb.toString());
			} else {
				StringBuffer sb = new StringBuffer();
				sb.append("<SUBSCRIBER>").append("<CUST_ID>").append(custId).append("</CUST_ID>").append("<LANG>")
						.append(lang).append("</LANG>").append("<BILL_CYCLE>").append(billcycle)
						.append("</BILL_CYCLE>");
				sb.append("<POSTPAID_LIST>").append("</POSTPAID_LIST>");
				sb.append("<PREPAID_LIST>").append(smallxml).append("</PREPAID_LIST>");

				if (mapOfEocXmls.get(msisdn) != null)
					sb.append("<EOC_LIST>").append(mapOfEocXmls.get(msisdn)).append("</EOC_LIST>");
				else
					sb.append("<EOC_LIST>").append("</EOC_LIST>");
				sb.append("</SUBSCRIBER>");
				// System.out.println(sb.toString());
				list.add(sb.toString());
				mapOfXmls.put(msisdn, sb.toString());

			}
		}
		return list;
	}

	public void generateCsv() {

		// TODO Auto-generated method stub
		try {
			File accountFile = new File(this.pathOfOutputFolder + "Account_" + uniqueNumber + ".csv");
			File subscriberFile = new File(this.pathOfOutputFolder + "Subscriber_" + uniqueNumber + ".csv");
			File offerFile = new File(this.pathOfOutputFolder + "Offer_" + uniqueNumber + ".csv");
			File offerAttrFile = new File(this.pathOfOutputFolder + "OfferAttribute_" + uniqueNumber + ".csv");
			File pamFile = new File(this.pathOfOutputFolder + "PamAccount_" + uniqueNumber + ".csv");
			File subsOfferFile = new File(this.pathOfOutputFolder + "SubscriberOffer_" + uniqueNumber + ".csv");
			File subsOfferAttrFile = new File(
					this.pathOfOutputFolder + "SubscriberOfferAttribute_" + uniqueNumber + ".csv");
			File providerOfferFile = new File(this.pathOfOutputFolder + "ProviderOffer_" + uniqueNumber + ".csv");
			File accumulatorFile = new File(this.pathOfOutputFolder + "Accumulator_" + uniqueNumber + ".csv");
			// File bigSubsFile = new File(this.pathOfOutputFolder +
			// "BigSubscriber.txt");

			File ucFile = new File(this.pathOfOutputFolder + "UsageCounter_" + uniqueNumber + ".csv");
			File utFile = new File(this.pathOfOutputFolder + "UsageThreshold_" + uniqueNumber + ".csv");
			File daFile = new File(this.pathOfOutputFolder + "DedicatedAccount_" + uniqueNumber + ".csv");
			File providerucFile = new File(this.pathOfOutputFolder + "ProviderUsageCounter_" + uniqueNumber + ".csv");
			File providerutFile = new File(this.pathOfOutputFolder + "ProviderUsageThreshold_" + uniqueNumber + ".csv");
			File rejectedFile = new File(this.pathOfLogFolder + "Rejected_"+uniqueNumber + ".log");
			File discardedFile = new File(this.pathOfLogFolder + "Error_" +uniqueNumber +".log");
			File onlyLogFile = new File(this.pathOfLogFolder + "Mismatch_" +uniqueNumber+ ".log");
			File notMigratedFile = new File(this.pathOfLogFolder + "NotMigrated_" +uniqueNumber+ ".log");

			if (!accountFile.exists()) {
				accountFile.createNewFile();
			}
			if (!subscriberFile.exists()) {
				subscriberFile.createNewFile();
			}
			if (!offerFile.exists()) {
				offerFile.createNewFile();
			}
			if (!offerAttrFile.exists()) {
				offerAttrFile.createNewFile();
			}
			if (!subsOfferFile.exists()) {
				subsOfferFile.createNewFile();
			}
			if (!subsOfferAttrFile.exists()) {
				subsOfferAttrFile.createNewFile();
			}
			if (!providerOfferFile.exists()) {
				providerOfferFile.createNewFile();
			}
			if (!pamFile.exists()) {
				pamFile.createNewFile();
			}
			if (!ucFile.exists()) {
				ucFile.createNewFile();
			}
			if (!utFile.exists()) {
				utFile.createNewFile();
			}
			if (!daFile.exists()) {
				daFile.createNewFile();
			}
			if (!providerucFile.exists()) {
				providerucFile.createNewFile();
			}
			if (!providerutFile.exists()) {
				providerutFile.createNewFile();
			}
			if (!rejectedFile.exists()) {
				rejectedFile.createNewFile();
			}
			if (!discardedFile.exists()) {
				discardedFile.createNewFile();
			}
			if (!onlyLogFile.exists()) {
				onlyLogFile.createNewFile();
			}
			if (!notMigratedFile.exists()) {
				notMigratedFile.createNewFile();
			}
			if (!accumulatorFile.exists()) {
				accumulatorFile.createNewFile();
			}
			/*
			 * if(!bigSubsFile.exists()){ bigSubsFile.createNewFile(); }
			 */
			FileUtils.writeLines(accountFile, this.accountListBuffer, true);
			FileUtils.writeLines(subscriberFile, this.subscriberListBuffer, true);
			FileUtils.writeLines(offerFile, this.offerListBuffer, true);
			FileUtils.writeLines(offerAttrFile, this.offerAttributeListBuffer, true);
			FileUtils.writeLines(pamFile, this.pamListBuffer, true);
			FileUtils.writeLines(subsOfferFile, this.subscriberOfferListBuffer, true);
			FileUtils.writeLines(subsOfferAttrFile, this.subscriberOfferAttributeListBuffer, true);
			FileUtils.writeLines(providerOfferFile, this.providerOfferListBuffer, true);
			FileUtils.writeLines(ucFile, this.ucBuffer, true);
			FileUtils.writeLines(utFile, this.utBuffer, true);
			FileUtils.writeLines(daFile, this.daBuffer, true);
			FileUtils.writeLines(providerucFile, this.providerucBuffer, true);
			FileUtils.writeLines(providerutFile, this.providerutBuffer, true);
			FileUtils.writeLines(accumulatorFile, this.accumulatorBuffer, true);
			
			//Remove duplicates from files
			
			
			FileUtils.writeLines(rejectedFile, rejectAndLog, true);	
			FileUtils.writeLines(discardedFile, discardAndLog, true);	
			FileUtils.writeLines(onlyLogFile, onlyLog, true);
			FileUtils.writeLines(notMigratedFile, notMigratedLog, true);
						
			

			// FileUtils.writeLines(bigSubsFile, this.smallFiles, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("IO Exception ", e);
		}

	}
	
	public synchronized void generateLogs() throws IOException{
		File rejectedFile = new File(this.pathOfLogFolder + "Rejected" + ".log");
		File discardedFile = new File(this.pathOfLogFolder + "Error" + ".log");
		File onlyLogFile = new File(this.pathOfLogFolder + "Mismatch" + ".log");
		File notMigratedFile = new File(this.pathOfLogFolder + "NotMigrated" + ".log");
		if (!rejectedFile.exists()) {
			rejectedFile.createNewFile();
		}
		if (!discardedFile.exists()) {
			discardedFile.createNewFile();
		}
		if (!onlyLogFile.exists()) {
			onlyLogFile.createNewFile();
		}
		if (!notMigratedFile.exists()) {
			notMigratedFile.createNewFile();
		}
		
		try {
			FileUtils.writeLines(rejectedFile, rejectAndLog, true);
		//	synchronized (discardAndLog) {
				FileUtils.writeLines(discardedFile, discardAndLog, true);	
			//}
			FileUtils.writeLines(onlyLogFile, onlyLog, true);
			FileUtils.writeLines(notMigratedFile, notMigratedLog, true);
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("IO Exception ", e);
		}	
				
	}
	
	public synchronized void clearLogs(){
		rejectAndLog.clear();
		//synchronized (discardAndLog) {
			discardAndLog.clear();
		//}	
		onlyLog.clear();
		notMigratedLog.clear();
	
	}

	public synchronized void  clearBuffer() {
		this.accountListBuffer.clear();
		this.subscriberListBuffer.clear();
		this.offerListBuffer.clear();
		this.offerAttributeListBuffer.clear();
		this.pamListBuffer.clear();
		this.subscriberOfferListBuffer.clear();
		this.subscriberOfferAttributeListBuffer.clear();
		this.providerOfferListBuffer.clear();
		this.ucBuffer.clear();
		this.utBuffer.clear();
		this.daBuffer.clear();
		this.providerucBuffer.clear();
		this.providerutBuffer.clear();
		this.accumulatorBuffer.clear();
		rejectAndLog.clear();
		discardAndLog.clear();	
		onlyLog.clear();
		notMigratedLog.clear();
		/*synchronized (rejectAndLog) {
			rejectAndLog.clear();
		}
		synchronized (discardAndLog) {
			discardAndLog.clear();	
		}
		synchronized (onlyLog) {
			onlyLog.clear();
		}
		synchronized (notMigratedLog) {
			notMigratedLog.clear();
		}			*/
		
	}
}
