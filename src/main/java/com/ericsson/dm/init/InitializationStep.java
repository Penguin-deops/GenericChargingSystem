package com.ericsson.dm.init;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.jdbc.core.JdbcTemplate;

import org.w3c.dom.Document;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
//import org.mapdb.HTreeMap;
//import org.mapdb.Serializer;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.ericsson.datamigration.imig.etl.cc.utils.DBConnectionPoolManager;
import com.ericsson.jibx.beans.BALANCELIST;

public class InitializationStep {

	public static final Set<String> invalidEquipId = new HashSet<>();
	public static final Map<String, List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO>> mapOfBalances = new ConcurrentHashMap<>(
			10000, 0.75f, 100);
	public static final Map<String, List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO>> mapOfBalances2Offers = new ConcurrentHashMap<>(
			10000, 0.75f, 100);
	public static final Map<String, String> equipId2PoMapPostPaid = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> equipId2PoMapPrePaid = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> po2OfferIdMap = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> OfferId2PoMap = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> commonConfigMap = new ConcurrentHashMap<>(100, 0.75f, 30);
	public static final Map<String, String> languageMap = new ConcurrentHashMap<>(100, 0.75f, 30);
	public static final Map<String, String> accountClassMap = new ConcurrentHashMap<>(100, 0.75f, 30);
	public static final Map<String, String> offerDefinitionMap = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> srcOfferDefinitionMap = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, Map<String, String>> offerAttributeDefinitionMap = new ConcurrentHashMap<>(10000,
			0.75f, 100);
	public static final Map<String, List<String>> DELTA_CUSTOMER_MAP_ADDITION = new ConcurrentHashMap<>(100000, 0.75f,
			100);

	public static final Map<String, Map<String, Set<String>>> DELTA_CUSTOMER_MAP_EXCHANGE = new ConcurrentHashMap<>(100000,
			0.75f, 100);
	public static final Map<String, Map<String, String>> DELTA_CUSTOMER_MAP_DELETION = new ConcurrentHashMap<>(100000,
			0.75f, 100);
	public static final Map<String, Integer> sourceULServiceCounter = new ConcurrentHashMap<>(1000, 0.75f, 100);
	public static final Map<String, Integer> targetULServiceCounter = new ConcurrentHashMap<>(1000, 0.75f, 100);
	public static final Map<String, String> sourceServiceCounter = new ConcurrentHashMap<>(1000, 0.75f, 100);
	public static final Map<String, String> targetServiceCounter = new ConcurrentHashMap<>(1000, 0.75f, 100);
	public static final Map<String, Integer> sourceServiceCounter1 = new ConcurrentHashMap<>(1000, 0.75f, 100);
	public static final Map<String, Integer> targetServiceCounter1 = new ConcurrentHashMap<>(1000, 0.75f, 100);
	
	public static final Map<String, String> usageCounterDefinitionMap = new ConcurrentHashMap<>(10000, 0.75f, 100);
	public static final Map<String, String> pucMap = new ConcurrentHashMap<>(10000, 0.75f, 10);
	public static final Map<String, String> putMap = new ConcurrentHashMap<>(10000, 0.75f, 10);
	public static final Map<String, String> daMap = new ConcurrentHashMap<>(10000, 0.75f, 10);
	public static final Map<String, String> deltaMsisdn = new ConcurrentHashMap<>(10000, 0.75f, 10);
	public static Random rand = new Random();
	final static Logger LOG = Logger.getLogger(InitializationStep.class);
	public static Document prepaidDocument = null, postpaidDocument = null;
	public static DB mapdb, dbForStEocItem;
	public String appxCntxtPath;
	public JdbcTemplate jdbctemplate;
	private String mapdbpath;
	public static String workingMode;
	// mapdbOffer, mapdb;

	public InitializationStep(String appxCntxtPath, String mapdbpath, String sdpid, String configPath,
			String dataFolderPath, String workingMode1) {

		try {
			workingMode = workingMode1;
			this.appxCntxtPath = appxCntxtPath;
			this.mapdbpath = mapdbpath;
			//Constants.DEFAULT_TIME_ZONE="Asia/Riyadh";
			DBConnectionPoolManager ob = new DBConnectionPoolManager();
			ob.initDBConnection(this.appxCntxtPath);
			System.out.println("--------------Migtool path :------------------------"+"/"+mapdbpath + "/db_" + sdpid);
			LOG.info("--------------Migtool path :------------------------"+"/"+mapdbpath + "/db_" + sdpid);
			mapdb = DBMaker.newFileDB(new File("/"+mapdbpath + "/db_" + sdpid)).mmapFileEnable().transactionDisable()
					.compressionEnable().asyncWriteEnable().commitFileSyncDisable().closeOnJvmShutdown().make();
			jdbctemplate = DBConnectionPoolManager.dbConnectionPoolManager.getImigJdbcTemplate();
			readXmlMappingDocument(dataFolderPath);
			readInvalidEquipmentId(dataFolderPath);
			readequipId2PoMapping(configPath);
			readpo2OfferMapping(dataFolderPath);
			commonConfigMapping(dataFolderPath);
			languageMapping(dataFolderPath);
			accountClassMapping(dataFolderPath);
			readOffer2POMapping(dataFolderPath);
			offerDefinitionMapping(configPath);
			offerAttributeDefinitionMapping(configPath);
			readUsageCounterDefinition(configPath);
			initializeProviderData(dataFolderPath);
			// sourceOfferDefinitionMapping(configPath);
			if (workingMode.equals("FULL_TRANSFORMATION")) {
				getDeltaCustomerDetails();
				loadSt_eoc_item2Mapdb();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("Exception occured ", e);
		}
	}

	private void readUsageCounterDefinition(String configPath) throws IOException {
		// TODO Auto-generated method stub
		final BufferedReader br = new BufferedReader(new FileReader(configPath + "/USAGE_COUNTER_DEFINITION.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			InitializationStep.usageCounterDefinitionMap.put(datas[0], datas[1] + "|" + datas[2]);
		}
		br.close();

	}

	private void readInvalidEquipmentId(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/InvalidEquipid.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			invalidEquipId.add(line.trim());
		}
		br.close();
	}

	private void readequipId2PoMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/EQUIP2PO.csv"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",",-3);
			String type = null; 
			if(datas!=null && datas.length>=3){
				type = datas[2];
			}
			if(type!=null){
				if(type.equalsIgnoreCase("POST")){
					if (InitializationStep.equipId2PoMapPostPaid.containsKey(datas[0].toUpperCase())) {
						String data = InitializationStep.equipId2PoMapPostPaid.get(datas[0].toUpperCase());
						data = data + "|" + datas[1].toUpperCase();
						InitializationStep.equipId2PoMapPostPaid.put(datas[0].toUpperCase().toUpperCase(), data);
					} else {
						InitializationStep.equipId2PoMapPostPaid.put(datas[0].toUpperCase(), datas[1].toUpperCase());
					}
				}
				else if(type.equalsIgnoreCase("PREP")){
					if (InitializationStep.equipId2PoMapPrePaid.containsKey(datas[0].toUpperCase())) {
						String data = InitializationStep.equipId2PoMapPrePaid.get(datas[0].toUpperCase());
						data = data + "|" + datas[1].toUpperCase();
						InitializationStep.equipId2PoMapPrePaid.put(datas[0].toUpperCase(), data);
					} else {
						InitializationStep.equipId2PoMapPrePaid.put(datas[0].toUpperCase(), datas[1].toUpperCase());
					}
				}
			}
		}
		br.close();
	}

	private void readpo2OfferMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/po2offer.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			if (InitializationStep.po2OfferIdMap.containsKey(datas[0])) {
				String data = InitializationStep.po2OfferIdMap.get(datas[0]);
				data = data + "|" + datas[1];
				InitializationStep.po2OfferIdMap.put(datas[0], data);
			} else {
				InitializationStep.po2OfferIdMap.put(datas[0], datas[1]);
			}
		}
		br.close();
	}

	private void commonConfigMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/Common_config.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split("\\|");
			InitializationStep.commonConfigMap.put(datas[0], datas[1]);
		}
		br.close();
	}

	private void languageMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/LanguageMapping.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			InitializationStep.languageMap.put(datas[0], datas[1]);
		}
		br.close();
	}

	private void accountClassMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/AccountClassMapping.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			InitializationStep.accountClassMap.put(datas[0], datas[1]);
		}
		br.close();
	}

	private void offerDefinitionMapping(String configPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(configPath + "/OFFER_DEFINITION.csv"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			InitializationStep.offerDefinitionMap.put(datas[0], datas[1] + "|" + datas[2]);
		}
		br.close();
	}

	private void sourceOfferDefinitionMapping(String configPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(configPath + "/SRC_OFFER_DEFINITION.csv"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			InitializationStep.srcOfferDefinitionMap.put(datas[0], datas[1] + "|" + datas[2]);
		}
		br.close();
	}

	private void offerAttributeDefinitionMapping(String configPath) {
		// TODO Auto-generated method stub
		BufferedReader br;
		String line = null;

		try {
			// BufferedWriter bw = new BufferedWriter(new
			// FileWriter(configPath+"/Converted_OFFER_ATTRIBUTE_DEFINITION.csv"));
			br = new BufferedReader(new FileReader(configPath + "/OFFER_ATTRIBUTE_DEFINITION.csv"));

			int counter = 0;
			while ((line = br.readLine()) != null) {
				String datas[] = line.split(",", -5);
				counter++;
				if (InitializationStep.offerAttributeDefinitionMap.containsKey(datas[0])) {
					if (datas.length > 4) {
						Map<String, String> map = InitializationStep.offerAttributeDefinitionMap.get(datas[0]);
						map.put(datas[1].toUpperCase(), datas[2] + "|" + datas[3] + "|" + datas[4]);
						// bw.write(datas[0]+","+datas[1].toUpperCase()+"|"+datas[2]
						// + "|" + datas[3] + "|" + datas[4]);
						InitializationStep.offerAttributeDefinitionMap.put(datas[0], map);
					}

				} else {
					if (datas.length > 4) {
						Map<String, String> map = new ConcurrentHashMap<>(10000, 0.75f, 100);
						map.put(datas[1].toUpperCase(), datas[2] + "|" + datas[3] + "|" + datas[4]);
						// bw.write(datas[0]+","+datas[1].toUpperCase()+"|"+datas[2]
						// + "|" + datas[3] + "|" + datas[4]);
						InitializationStep.offerAttributeDefinitionMap.put(datas[0], map);
					}
				}
			}
			br.close();
			// bw.close();
			System.out.println("Counter " + counter);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(line);
			e.printStackTrace();
		}
	}

	private void readOffer2POMapping(String dataFolderPath) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(dataFolderPath + "/offer2po.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String datas[] = line.split(",");
			if (InitializationStep.OfferId2PoMap.containsKey(datas[0])) {
				String data = InitializationStep.OfferId2PoMap.get(datas[0]);
				data = data + "|" + datas[1];
				InitializationStep.OfferId2PoMap.put(datas[0], data);
			} else {
				InitializationStep.OfferId2PoMap.put(datas[0], datas[1]);
			}
		}
		br.close();
	}

	/*
	 * public InitializationStepPA17(String appxCntxtPath, String mapdbpath,
	 * String sdpid, String outputFolderPath, String dataFolderPath, String
	 * type) {
	 * 
	 * mapdbOffer = DBMaker .newFileDB(new File(mapdbpath + "/db_Offer_" +
	 * Calendar.getInstance().getTimeInMillis() + "_" + sdpid))
	 * .mmapFileEnable().transactionDisable().compressionEnable().
	 * deleteFilesAfterClose().asyncWriteEnable()
	 * .commitFileSyncDisable().closeOnJvmShutdown().make(); mapdb =
	 * DBMaker.newFileDB(new File(mapdbpath + "/db_" +
	 * sdpid)).mmapFileEnable().transactionDisable()
	 * .compressionEnable().asyncWriteEnable().commitFileSyncDisable().
	 * closeOnJvmShutdown().make();
	 * 
	 * try { loadOfferDataIntoMapDB(mapdbOffer, "OFFER_DEFN", new
	 * File(outputFolderPath + "/Offer.csv"));
	 * loadOfferDataIntoMapDB(mapdbOffer, "PROVIDER_OFFER_DEFN", new
	 * File(outputFolderPath + "/SubscriberOffer.csv"));
	 * mapOfPostpaidBalances.clear(); mapOfPrepaidBalances.clear();
	 * 
	 * readXmlMappingDocument(dataFolderPath); } catch (Exception e) { // TODO
	 * Auto-generated catch block LOG.error("Exception occured ", e);
	 * e.printStackTrace(); }
	 * 
	 * }
	 */

	/*
	 * private void loadOfferDataIntoMapDB(DB db, String entity, File fullFile)
	 * throws Exception { // TODO Auto-generated method stub //
	 * System.out.println("INVOKED loadIntoMapDB"); HTreeMap<Object, Object>
	 * hTree = db.createHashMap(entity).keySerializer(Serializer.STRING)
	 * .valueSerializer(Serializer.STRING).make(); if (fullFile.exists()) {
	 * BufferedReader br = new BufferedReader(new FileReader(fullFile)); String
	 * line; while ((line = br.readLine()) != null) { String arr[] =
	 * line.split(",", -8); String msisdn = arr[0]; if
	 * (hTree.containsKey(msisdn)) { hTree.put(msisdn, hTree.get(msisdn) + "|" +
	 * arr[1] + "," + arr[7] + "," + arr[8]); } else { hTree.put(msisdn, arr[1]
	 * + "," + arr[7] + "," + arr[8]); } } br.close(); } // hTree.close();;
	 * 
	 * }
	 */
	private void readXmlMappingDocument(String dataFolderPath) {
		IBindingFactory bindingFactoryBalance = null;
		IUnmarshallingContext unmarshallingContextBalance;
		try {
			bindingFactoryBalance = BindingDirectory.getFactory(com.ericsson.jibx.beans.BALANCELIST.class);
			unmarshallingContextBalance = bindingFactoryBalance.createUnmarshallingContext();
			BALANCELIST balancelist = (com.ericsson.jibx.beans.BALANCELIST) unmarshallingContextBalance
					.unmarshalDocument(new ByteArrayInputStream(
							FileUtils.readFileToByteArray(new File(dataFolderPath + "/Balances.xml"))), null);
			for (com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO balanceInfo : balancelist.getBALANCEINFOList()) {
				if (mapOfBalances.containsKey(balanceInfo.getEQUIPID())) {
					List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO> listOfBalanceInfo = mapOfBalances
							.get(balanceInfo.getEQUIPID());
					listOfBalanceInfo.add(balanceInfo);
					mapOfBalances.put(balanceInfo.getEQUIPID(), listOfBalanceInfo);
				} else {
					List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO> listOfBalanceInfo = new ArrayList<>();
					listOfBalanceInfo.add(balanceInfo);
					mapOfBalances.put(balanceInfo.getEQUIPID(), listOfBalanceInfo); 
				}
			}
		   balancelist = (com.ericsson.jibx.beans.BALANCELIST) unmarshallingContextBalance
					.unmarshalDocument(new ByteArrayInputStream(
							FileUtils.readFileToByteArray(new File(dataFolderPath + "/BalancesOffer.xml"))), null);
			for (com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO balanceInfo : balancelist.getBALANCEINFOList()) {
				if (mapOfBalances2Offers.containsKey(balanceInfo.getOFFERID())) {
					List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO> listOfBalanceInfo = mapOfBalances2Offers
							.get(balanceInfo.getOFFERID());
					listOfBalanceInfo.add(balanceInfo);
					mapOfBalances2Offers.put(balanceInfo.getOFFERID(), listOfBalanceInfo);
				} else {
					List<com.ericsson.jibx.beans.BALANCELIST.BALANCEINFO> listOfBalanceInfo = new ArrayList<>();
					listOfBalanceInfo.add(balanceInfo);
					mapOfBalances2Offers.put(balanceInfo.getOFFERID(), listOfBalanceInfo); 
				}
			}


		} catch (JiBXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void getDeltaCustomerDetails() {
		List<Map<String, Object>> lmap = jdbctemplate.queryForList("select MSISDN,OLD_EQUIP_ID,NEW_EQUIP_ID,TO_CHAR(VALID_FROM,'yyyy-MM-dd hh24:mi:ss') VALID_FROM,TO_CHAR(VALID_TO,'yyyy-MM-dd hh24:mi:ss') VALID_TO from STG_ADDON_OPT_IN_OUT");
		for (Map<String, Object> map : lmap) {
			String oldEquipId = (String) map.get("OLD_EQUIP_ID");
			String newEquipId = (String) map.get("NEW_EQUIP_ID");
			String validFrom = (String) map.get("VALID_FROM");
			String validTo = (String) map.get("VALID_TO");
			String msisdn = (String) map.get("MSISDN");
			oldEquipId=oldEquipId!=null && oldEquipId.length()>0?oldEquipId.toUpperCase():oldEquipId;
			newEquipId=newEquipId!=null && newEquipId.length()>0?newEquipId.toUpperCase():newEquipId;
			if (oldEquipId == null && newEquipId != null) {
				if (DELTA_CUSTOMER_MAP_ADDITION.containsKey(msisdn)) {
					List<String> temp = DELTA_CUSTOMER_MAP_ADDITION.get(msisdn);
					temp.add(newEquipId + "," + validFrom + "," + validTo);
					DELTA_CUSTOMER_MAP_ADDITION.put(msisdn, temp);
				} else {
					List<String> temp = new ArrayList<>();
					temp.add(newEquipId + "," + validFrom + "," + validTo);
					DELTA_CUSTOMER_MAP_ADDITION.put(msisdn, temp);
				}
			} else if (oldEquipId != null && newEquipId != null) {
				if (DELTA_CUSTOMER_MAP_EXCHANGE.containsKey(msisdn)) {
					Map<String, Set<String>> temp = DELTA_CUSTOMER_MAP_EXCHANGE.get(msisdn);
					if(temp.containsKey(oldEquipId)){
						Set<String> set = temp.get(oldEquipId);
						set.add(newEquipId + "," + validFrom + "," + validTo);
						temp.put(oldEquipId, set);
					}
					else{
						Set<String> set = new HashSet<>();
						set.add(newEquipId + "," + validFrom + "," + validTo);
						temp.put(oldEquipId, set);
					}
					DELTA_CUSTOMER_MAP_EXCHANGE.put(msisdn, temp);
				} else {
					Map<String, Set<String>> temp = new HashMap<>();
					Set<String> set = new HashSet<>();
					set.add(newEquipId + "," + validFrom + "," + validTo);
					temp.put(oldEquipId, set);
					DELTA_CUSTOMER_MAP_EXCHANGE.put(msisdn, temp);
				}
			} else if (oldEquipId != null && newEquipId == null) {
				if (DELTA_CUSTOMER_MAP_DELETION.containsKey(msisdn)) {
					Map<String, String> temp = DELTA_CUSTOMER_MAP_DELETION.get(msisdn);
					temp.put(oldEquipId, "EMPTY");
					DELTA_CUSTOMER_MAP_DELETION.put(msisdn, temp);
				} else {
					Map<String, String> temp = new HashMap<>();
					temp.put(oldEquipId, "EMPTY");
					DELTA_CUSTOMER_MAP_DELETION.put(msisdn, temp);
				}
			}

		}
	}

	private void loadSt_eoc_item2Mapdb() throws Exception {
		// TODO Auto-generated method stub
		// System.out.println("INVOKED loadIntoMapDB");
		dbForStEocItem = DBMaker.newFileDB(new File(mapdbpath + "/db_eoc_item")).mmapFileEnable().transactionDisable()
				.compressionEnable().asyncWriteEnable().commitFileSyncDisable().closeOnJvmShutdown().make();
		HTreeMap<String, String> hTree = dbForStEocItem.createHashMap("eoc_item").keySerializer(Serializer.STRING)
				.valueSerializer(Serializer.STRING).make();
		List<Map<String, Object>> lmap = jdbctemplate
				.queryForList("select * from ST_EOC_ITEM_DELTA where itemtype = 'ProductOffering'");
		for (Map<String, Object> map : lmap) {
			String itemId =  (BigDecimal)map.get("ITEMID")+"";
			String itemCode = (String) map.get("ITEMCODE");
			String msisdn = (String) map.get("MSISDN");
			msisdn = msisdn.substring(3, msisdn.length());
			if (hTree.containsKey(msisdn)) {
				String temp = hTree.get(msisdn);
				temp = temp + "|" + itemId + "," + itemCode;
				hTree.put(msisdn, temp);
				//LOG.info(msisdn+","+temp);
			} else {
				hTree.put(msisdn, itemId + "," + itemCode);
				//LOG.info(msisdn+","+ itemId + "," + itemCode);
			}

		}
		dbForStEocItem.commit();

	}

	private  void initializeProviderData(String dataPath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(dataPath + "/CreditLimit.txt"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String data[] = line.split(",", -4);
			if (data != null && data.length > 7) {
				String type = data[0].trim();
				String classs = data[1].trim();
				String ucid = data[2] != null ? data[2].trim() : null;
				String utid = data[3] != null ? data[3].trim() : null;
				String daid = data[4] != null ? data[4].trim() : null;
				String ignore = data[5].trim();
				String targetucid = data[6] != null ? data[6].trim() : null;
				String targetutid = data[7] != null ? data[7].trim() : null;
				String targetdaid = data[8] != null ? data[8].trim() : null;

				if (ucid != null) {
					//String ucdata= targetucid;
					// if map already contains data then update the key
					if(InitializationStep.pucMap.containsKey(type + "_" + classs + "_" + ucid + "_" + ignore)){
						  String ucdata  = InitializationStep.pucMap.get(type + "_" + classs + "_" + ucid + "_" + ignore);
						  ucdata = ucdata+","+targetucid;
						  InitializationStep.pucMap.put(type + "_" + classs + "_" + ucid + "_" + ignore,ucdata);
					}
					else{
					InitializationStep.pucMap.put(type + "_" + classs + "_" + ucid + "_" + ignore, targetucid);
					}
				}
				if (utid != null) {
					//String utdata= targetutid;
					if(InitializationStep.putMap.containsKey(type + "_" + classs + "_" + utid + "_" + ignore)){
						  String utdata  = InitializationStep.putMap.get(type + "_" + classs + "_" + utid + "_" + ignore);
						  utdata = utdata+","+targetutid;
						  InitializationStep.putMap.put(type + "_" + classs + "_" + utid + "_" + ignore,utdata);
						  
					}
					else{
					InitializationStep.putMap.put(type + "_" + classs + "_" + utid + "_" + ignore, targetutid);
					
					}
				}
				if (daid != null) {
					String dadata= targetdaid;
					if(InitializationStep.daMap.containsKey(type + "_" + classs + "_" + daid + "_" + ignore)){
						dadata  = InitializationStep.daMap.get(type + "_" + classs + "_" + daid + "_" + ignore);
						dadata = dadata+","+targetdaid;
						InitializationStep.daMap.put(type + "_" + classs + "_" + daid + "_" + ignore,dadata);
					}
					else{
					InitializationStep.daMap.put(type + "_" + classs + "_" + daid + "_" + ignore, daid);
					}
				}
			}
		}
		br.close();
	}
	/*
	 * private void initULSource(String dataFolder){
	 * 
	 * try { BufferedReader br = new BufferedReader (new
	 * FileReader(dataFolder+"/Reconsource.txt"));
	 * 
	 * } catch (FileNotFoundException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); }
	 * 
	 * }
	 */

}
