package com.codect.writers;

//import static org.mockito.ArgumentMatchers.eq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import static com.mongodb.client.model.Filters.*;

import com.codect.common.Fields;
import com.codect.common.MLog;
import com.codect.connections.MongoConnection;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

public class UpsertMongoWriter extends Writer {
	String colName;
	MongoCollection<Document> col;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(List<Map<String, Object>> next) {
		List<WriteModel<Document>> batch = new ArrayList<WriteModel<Document>>();
		ArrayList<String> keys = (ArrayList<String>) target.get("keys");
		// set
		String toAdd = (String) target.get("set");
		// import the whole data and iterate over it ,and update
		for (Map<String, Object> map : next) {
			Document filter = new Document();
			for (String key : keys) {
				filter.put(key, map.get(key));				
			}
			Document dbObject = new Document(map);
			dbObject.append(toAdd, map.get(toAdd));
			UpdateManyModel<Document> oneOperation = 
					new UpdateManyModel<>(filter, new Document("$set", dbObject),
					new UpdateOptions().upsert(true));
			batch.add(oneOperation);
		}
		 col.bulkWrite(batch);
	}

	@Override
	public void init() {
		colName = (String) target.get("collection");
		col = MongoConnection.getInstance().getCollection(colName);
	}

	@Override
	public void prepareTarget(Map<String, Object> map) {

	}
}
