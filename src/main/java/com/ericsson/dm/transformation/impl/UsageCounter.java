package com.ericsson.dm.transformation.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ericsson.jibx.beans.SUBSCRIBER;

public class UsageCounter {
	SUBSCRIBER subscriber;
	List<String> rejectAndLog, discardAndLog, onlyLog;
	Set<String> validMsisdn;

	public UsageCounter(SUBSCRIBER subscriber, Set<String> validMsisdn, List<String> rejectAndLog,
			List<String> discardAndLog, List<String> onlyLog, List<String> notMigratedLog) {
		this.subscriber = subscriber;
		this.rejectAndLog = rejectAndLog;
		this.discardAndLog = discardAndLog;
		this.onlyLog = onlyLog;
		this.validMsisdn = validMsisdn;
		// TODO Auto-generated constructor stub
	}

	public Map<String, List<String>> execute() {
		// TODO Auto-generated method stub
		Map<String, List<String>> map = new HashMap<>();
		map.put("UsageCounter", generatUsageCounter());
		map.put("ProviderUsageCounter", generateProviderUsageCounter());

		return map;
	}

	private List<String> generatUsageCounter() {

		return new ArrayList<>();
	}

	private List<String> generateProviderUsageCounter() {

		return new ArrayList<>();
	}

	

}
