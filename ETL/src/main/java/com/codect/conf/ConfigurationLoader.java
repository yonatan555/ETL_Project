package com.codect.conf;

import java.util.Map;

public abstract class ConfigurationLoader {

    public static ConfigurationLoader getInstance() throws RuntimeException{
    	return new MongoConfigurationLoader();
    }

    public abstract Map<String,Object> readConfFor(String taskName);
    
    public abstract void saveConfFor(String taskName, Map<String,Object> source, Map<String,Object> target);
    
	public abstract Object getGlobalProperty(String key);

	public void saveMax(String string, Map<String, Object> maxFromConfig) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getMax(String string) {
		// TODO Auto-generated method stub
		return null;
	}
}