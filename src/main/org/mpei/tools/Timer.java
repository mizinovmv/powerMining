package org.mpei.tools;

import java.util.LinkedHashMap;
import java.util.Map;

public class Timer {
	private long startTime = 0;

	private String msg = null;

	private Map<String, Long> map = new LinkedHashMap<String, Long>();

	public void start(String msg) {
		if (startTime != 0) {
			throw new IllegalStateException("Already started");
		}
		startTime = System.currentTimeMillis();
		this.msg = msg;
	}

	public void stop() {
		if (startTime == 0) {
			throw new IllegalStateException("Not started");
		}
		long now = System.currentTimeMillis();
		Long n = map.get(msg);
		if (n == null) {
			n = 0l;
		}
		n += (now - startTime);
		map.put(msg, n);
		startTime = 0;
		msg = null;
	}

	public void output() {
		for (String msg : map.keySet()) {
			System.out.println(msg + ": " + map.get(msg));
		}
	}
}
