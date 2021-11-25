package com.codect.readers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import com.codect.common.DBObjectUtil;
import com.codect.transforms.Transformer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WebServiceReader extends Reader {
	List<Map<String, Object>> streets ;
	
	@Override
	public boolean hasNext() {	
		return !streets.isEmpty();
	}

	@Override
	public List<Map<String, Object>> next() {
		List<Map<String, Object>> streets2 = new ArrayList<>(streets);
		streets.clear();
		return streets2;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void init() {
		RestTemplate restTemplate = new RestTemplate();
		String url = (String) source.get("url");
		ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
		streets = (List<Map<String, Object>>) DBObjectUtil.getInnerField("result.records", response.getBody());
	}
}
