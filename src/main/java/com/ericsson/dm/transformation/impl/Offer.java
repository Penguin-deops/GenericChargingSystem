package com.ericsson.dm.transformation.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.ericsson.jibx.beans.SUBSCRIBER;

public class Offer {
	 SUBSCRIBER subscriber;	
	 List<String> rejectAndLog, discardAndLog, onlyLog;
	 Set<String> validMsisdn;
	public Offer(SUBSCRIBER subscriber, Set<String> validMsisdn, List<String> rejectAndLog, List<String> discardAndLog,
			List<String> onlyLog) {
		// TODO Auto-generated constructor stub
		this.subscriber=subscriber;
		this.rejectAndLog=rejectAndLog;
		this.discardAndLog=discardAndLog;
		this.onlyLog=onlyLog;
		this.validMsisdn=validMsisdn;
	}
	public Map<String, List<String>> execute() {
		// TODO Auto-generated method stub
		Map<String,List<String>> map = new HashMap<>();
		map.put("Offer", generateOffers());
		map.put("OfferAttribute", generateOfferAttributes());
		map.put("Pam", generatePam());
		
		return map;
	}
	
	private List<String> generateOffers(){
		
		return new ArrayList<>();
	}
	
	private List<String> generateOfferAttributes(){
		
		return new ArrayList<>();
	}

	private List<String> generatePam(){
		
		return new ArrayList<>();
	}


}
