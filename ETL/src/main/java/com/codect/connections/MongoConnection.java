package com.codect.connections;

import java.net.UnknownHostException;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoConnection {
	private MongoClient client;
	private static MongoConnection instance = new MongoConnection();

	private MongoConnection() {
		try {
			client = createClient();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public static MongoConnection getInstance() {
		return instance;
	}

	private MongoClient createClient() throws UnknownHostException {
		client = MongoClients.create("mongodb+srv://bigData:Daniel10@cluster0"
				+ ".anrrn.mongodb.net/university?retryWrites=true&w=majority");
		return client;
	}

	public MongoDatabase getDb() {
		return client.getDatabase("university");
	}

	public MongoCollection<Document> getCollection(String collectionName) {
		return client.getDatabase("university").getCollection(collectionName);
	}
}