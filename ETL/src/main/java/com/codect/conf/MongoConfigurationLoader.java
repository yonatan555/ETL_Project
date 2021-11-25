package com.codect.conf;

import java.util.Map;

import com.codect.common.Fields;
import com.codect.connections.MongoConnection;
import com.mongodb.BasicDBObject;

public class MongoConfigurationLoader extends ConfigurationLoader {

	@Override
	public Map<String,Object> readConfFor(String taskName) {
		return MongoConnection.getInstance().getCollection(Fields.Tasks).find(new BasicDBObject("_id", taskName)).first();
	}

	@Override
	public Object getGlobalProperty(String key) {
		return null;
	}

	@Override
	public void saveConfFor(String taskName, Map<String, Object> source, Map<String, Object> target) {
		Map<String, Object> conf = readConfFor(taskName);
		conf.put(Fields.source, source);
		conf.put(Fields.target, target);
		MongoConnection.getInstance().getCollection(Fields.Tasks).
			updateOne(new BasicDBObject(Fields._id,taskName),new BasicDBObject(conf));
	}
}