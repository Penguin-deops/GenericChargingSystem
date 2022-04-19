package com.ericsson.dm.transformation.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


import com.ericsson.dm.utils.Constants;
import com.ericsson.jibx.beans.POSTPAIDLIST.POSTPAIDINFO;
import com.ericsson.jibx.beans.SUBSCRIBER;

public class Account {
    SUBSCRIBER subscriber;	
	List<String> rejectAndLog, discardAndLog, onlyLog;
	Set<String> validMsisdn;
     

	public Account(SUBSCRIBER subscriber, Set<String> validMsisdn, List<String> rejectAndLog, List<String> discardAndLog, List<String> onlyLog) {
		this.subscriber=subscriber;
		this.rejectAndLog=rejectAndLog;
		this.discardAndLog=discardAndLog;
		this.onlyLog=onlyLog;
		this.validMsisdn=validMsisdn;
		
	}


	public Collection<? extends String> execute() {
		// TODO Auto-generated method stub
		List<String> result = new ArrayList<String>();
		for(com.ericsson.jibx.beans.SUBSCRIBER.POSTPAIDLIST.POSTPAIDINFO ppinfo : subscriber.getPOSTPAIDLIST1().getPOSTPAIDINFOList()){
			
		}
		return result;
		
	}
	private List<String> applyRulesAccount(String msisdn, String serviceClass, long actDate,
			com.ericsson.jibx.beans.AccountList account) {
		List<String> accountList = new ArrayList<>();
		StringBuffer sb = new StringBuffer();
		String sfeedate = "0";
		String supdate = "0";
		String communityId1 = "0";
		String communityId2 = "0";
		String communityId3 = "0";
		String units = "0";
		String sfeeStatus = "0";
		String supStatus = "0";

		// validTo = ruleB(validTo);
		sb.append(msisdn).append(",");
		sb.append(serviceClass).append(",");
		sb.append(serviceClass).append(",");
		sb.append(Constants.DEFAULT_NULL).append(",");
		sb.append(units).append(",");
		sb.append(actDate).append(",");
		sb.append(sfeedate).append(",");
		sb.append(supdate).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(sfeeStatus).append(",");
		sb.append(supStatus).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ONE).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(communityId1).append(",");
		sb.append(communityId2).append(",");
		sb.append(communityId3).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(50).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_NULL);
		accountList.add(sb.toString());
		
		sb = null;
		return accountList;
	}
	
}
