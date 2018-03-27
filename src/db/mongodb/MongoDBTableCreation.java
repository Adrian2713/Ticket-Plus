package db.mongodb;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
public class MongoDBTableCreation {
	// Run as Java application to create MongoDB collections with index.
	  public static void main(String[] args) {

	    // Step 1: Connection to MongoDB
		  MongoClient mongoclient = new MongoClient("localhost", 27017);
		  MongoDatabase db = mongoclient.getDatabase(MongoDBUtil.DB_NAME);
	    // Step 2: remove old collections.
		  db.getCollection("users").drop();
		  db.getCollection("items").drop();

	    // Step 3: create new collections. 相比于mysql 没有schema
		  IndexOptions indexOptions = new IndexOptions().unique(true);
		  db.getCollection("users").createIndex(new Document("user_id", 1), indexOptions);
		  db.getCollection("items").createIndex(new Document("item_id", 1), indexOptions);
	    // Step 4: insert fake user data
		  db.getCollection("users").insertOne(new Document().append("first_name", "John").append("last_name", "Smith")
					.append("user_id", "1111").append("password", "3229c1097c00d497a0fd282d586be050"));

			mongoclient.close();
			System.out.println("Import is done successfully.");
		  
	    System.out.println("Import is done successfully.");
	  }
}
