package com.ericsson.dm.transformation.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.ericsson.dm.utils.Constants;
import com.ericsson.jibx.beans.SUBSCRIBER;
import com.ericsson.jibx.beans.SubscriberList;

public class Subscriber {
	 SUBSCRIBER subscriber;	
    List<String> rejectAndLog, discardAndLog, onlyLog;
    Set<String> validMsisdn;
	public Subscriber(SUBSCRIBER subscriber,Set<String> validMsisdn, List<String> rejectAndLog, List<String> discardAndLog, List<String> onlyLog) {
		// TODO Auto-generated constructor stub
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

	private List<String> applyRulesSubscribers(List<String> subsList, String msisdn, String lang, long actDate,SubscriberList subscriber) {
		String firstIvrCallDone=""+actDate;
		String subsStatus = "1";
		if(subscriber!=null && subscriber.getSubscriberInfoList().size()>0){
			firstIvrCallDone=subscriber.getSubscriberInfoList().get(0).getFirstCallDone();
			subsStatus=subscriber.getSubscriberInfoList().get(0).getSubscriberStatus();
		}
		StringBuffer sb = new StringBuffer();
		sb.append(msisdn).append(",");
		sb.append(msisdn).append(",");
		sb.append(subsStatus).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ONE).append(",");
		sb.append(firstIvrCallDone).append(",");
		sb.append(lang).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append(Constants.DEFAULT_ZERO).append(",");
		sb.append("99").append(",");
		sb.append("").append(",");
		sb.append(Constants.DEFAULT_ZERO);
		subsList.add(sb.toString());
		sb = null;
		return subsList;
	}
}
