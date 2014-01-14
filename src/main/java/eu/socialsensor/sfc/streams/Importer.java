package eu.socialsensor.sfc.streams;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

import com.flickr4java.flickr.people.User;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.abstractions.flickr.FlickrStreamUser;
import eu.socialsensor.framework.abstractions.instagram.InstagramItem;
import eu.socialsensor.framework.abstractions.twitpic.TwitPicUser;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.framework.common.factories.ObjectFactory;
import eu.socialsensor.framework.retrievers.Retriever;
import eu.socialsensor.framework.retrievers.facebook.FacebookRetriever;
import eu.socialsensor.framework.retrievers.flickr.FlickrRetriever;
import eu.socialsensor.framework.retrievers.instagram.InstagramRetriever;
import eu.socialsensor.framework.retrievers.twitpic.TwitpicMediaRetriever;
import eu.socialsensor.framework.retrievers.youtube.YtMediaRetriever;

public class Importer {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		System.out.println("Index to solr");
		
		//fixClusters();
		indexToSolr();
		System.exit(0);
		
		//addMediaShares();
		//System.out.println("Done");
		//System.exit(0);
		
		//String instagramToken = "342704836.5b9e1e6.503a35185da54224adaa76161a573e71";
		//String instagramSecret = "e53597da6d7749d2a944651bbe6e6f2a";
		
		//String youtubeClientId = "manosetro";
		//String youtubeDevKey = "AI39si6DMfJRhrIFvJRv0qFubHHQypIwjkD-W7tsjLJArVKn9iR-QoT8t-UijtITl4TuyHzK-cxqDDCkCBoJB-seakq1gbt1iQ";
		
		//String flickrKey = "029eab4d06c40e08670d78055bf61205";
		//String flickrSecret = "bc4105126a4dfb8c";
		
		String facebookKey = "029eab4d06c40e08670d78055bf61205";
		String facebookSecret = "bc4105126a4dfb8c";
		
		
		//YtMediaRetriever retriever = new YtMediaRetriever(youtubeClientId, youtubeDevKey);
		//InstagramRetriever retriever = new InstagramRetriever(instagramSecret, instagramToken, 1, 1);
		//TwitpicMediaRetriever retriever = new TwitpicMediaRetriever();
		//FlickrRetriever retriever = new FlickrRetriever(flickrKey, flickrSecret, 1, 1);
		//FacebookRetriever retriever = new FacebookRetriever(facebookKey, facebookSecret, 1, 1);
		
		MongoClient client1 = new MongoClient("localhost");
		DB db1 = client1.getDB("mmdemo");
		//DBCollection miColl1 = db1.getCollection("MediaItems");
		DBCollection suColl1 = db1.getCollection("StreamUsers");
		
		//MediaItemDAO dao = new MediaItemDAOImpl("localhost", "mmdemo", "MediaItems");
		
		//StreamUserDAO udao = new StreamUserDAOImpl("localhost", "mmdemo", "StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", "Flickr");
		//suColl1.remove(query);
		
		DBCursor cursor = suColl1.find(query);
		System.out.println(cursor.count() + " stream users found");
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k%10==0)
				System.out.println(k+" media items processed!");
			
			DBObject obj = cursor.next();
			
			String id = (String) obj.get("id");
			String name = (String) obj.get("name");
			
			/* 
			//String ref = (String) obj.get("reference");
			//System.out.println(ref);
			
			String id = (String) obj.get("id");
			
			//String url = (String) obj.get("url");
			String iid = id.substring(id.indexOf("#")+1);
			//System.out.println(id+" => "+iid + " => " + url);
			
			MediaItem mi = null;
			try {
				mi = retriever.getMediaItem(iid);
				if(mi==null)
					throw new Exception();
			}
			catch(Exception e) {
				DBObject q = new BasicDBObject("id", id);
				DBObject o = new BasicDBObject("$set", new BasicDBObject("uid", "X"));
				
				miColl1.update(q, o);
				continue;
			}
			
			StreamUser user = mi.getUser();
			if(user!=null) {
				udao.insertStreamUser(user);
				//System.out.println(id+" => "+mi.getUserId());
				DBObject q = new BasicDBObject("id", id);
				DBObject o = new BasicDBObject("$set", new BasicDBObject("uid", user.getId()));
				
				miColl1.update(q, o);
			}
			else {
				System.out.println("User is null for " + id);
			}
			
			*/

			/*
			String id = (String) obj.get("id");
			
			String uid = (String) obj.get("uid");
			//System.out.println(uid);
			
			String name = uid.substring(uid.indexOf("#")+1);
			System.out.println(name);
			
			DBObject user = suColl1.findOne(new BasicDBObject("name", name));
			Object id2 = user.get("id");
			
			miColl1.update(new BasicDBObject("id", id), new BasicDBObject("$set", new BasicDBObject("uid", id2)));
			*/
			
			
			//String json = obj.toString();
			//Map<String, Integer> popularity = (Map<String, Integer>) obj.get("popularity");
			
			/* */
			
			//StreamUser user = ObjectFactory.createUser(json);
			//user.setId(user.getId().replaceAll("::", "#"));
			
			
			
			
			//User temp = retriever.retrieveUser(userid);
			//StreamUser user = new FacebookStreamUser(temp);
			
//			System.out.println(user);
//			if(popularity!=null) {
//				Integer followers = popularity.get("followers");
//				Integer friends = popularity.get("friends");
//				if(followers==null)
//					followers = 0;
//				if(friends==null)
//					friends = 0;
//			
//				user.setFollowers((long)followers);
//				user.setFriends((long)friends);
//			}
			
			//String pageUrl = "https://www.facebook.com/"+user.getUserid();
			//user.setPageUrl(pageUrl);
			//user.setProfileImage("http://graph.facebook.com/" + user.getUserid() + "/picture");

			//udao.insertStreamUser(user);
			
			
			
			
			/* 
			
			String uid = "Facebook#"+obj.get("author").toString();
			 
			MediaItem mi = ObjectFactory.createMediaItem(json);
			Integer shares = popularity.get("shares");
			if(shares==null)
				shares = 0;
			Integer likes = popularity.get("likes");
			if(likes==null)
				likes = 0;
			Integer comments = popularity.get("comments");
			if(comments==null)
				comments = 0;
			
			mi.setShares((long)shares);
			mi.setLikes((long)likes);
			mi.setViews(0L);
			mi.setComments((long)comments);
			
			mi.setUserId(uid);
			
			mi.setId(mi.getId().replaceAll("::", "#"));
			mi.setRef(mi.getRef().replaceAll("::", "#"));
			System.out.println(mi.toJSONString());
			dao.addMediaItem(mi);
			*/
			
			DBObject q = new BasicDBObject("id", id);
			DBObject o = new BasicDBObject("$set", new BasicDBObject("username", name));
		 	
		}
		
	}

	public static void fixClusters() throws UnknownHostException {
		
		MongoClient client = new MongoClient("160.40.51.18");
		DB db = client.getDB("gezi");
		DBCollection coll = db.getCollection("MediaItemClusters");
	
		DBCursor cursor = coll.find().sort(new BasicDBObject("count", -1));
		System.out.println(cursor.count() + " stream users found");
		int k=0;
		while(cursor.hasNext()) {
			
			if(k++%1000==0)
				System.out.println(k+" items processed!");
			
			DBObject obj = cursor.next();
			
			String id = (String) obj.get("id");
			Integer count = (Integer) obj.get("count");
			List<String> members = (List<String>) obj.get("members");
			
			if(count != members.size()) {
				System.out.println(count + " =/=" + members.size());
			}
			
			//System.out.println(id+" =>" + members.size());
			//BasicDBObject query = new BasicDBObject("id",id);
				
			//DBObject doc = new BasicDBObject("$set", new BasicDBObject("count", members.size()));
			//coll.update(query, doc);
			
			

		}
	}

	public static void addMediaShares() throws UnknownHostException {
		
		MongoClient client = new MongoClient("160.40.50.207");
		DB db = client.getDB("mmdemo");
		DBCollection iColl = db.getCollection("Items");
		
		MongoClient client2 = new MongoClient("160.40.50.207");
		DB db2 = client2.getDB("mmdemo");
		DBCollection sharesColl = db2.getCollection("MediaItemsShares");

		//MediaItemDAO dao = new MediaItemDAOImpl("localhost", "mmdemo", "MediaItems");
		
		//StreamUserDAO udao = new StreamUserDAOImpl("localhost", "mmdemo", "StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", "Twitter");
		
		DBCursor cursor = iColl.find(query);
		System.out.println(cursor.count() + " stream users found");
		int k=0;
		while(cursor.hasNext()) {
			
			if(k++%1000==0)
				System.out.println(k+" items processed!");
			
			DBObject obj = cursor.next();
			String json = obj.toString();
			String uid = (String) obj.get("uid");
			
			Item item = ObjectFactory.createItem(json);
			
			String ref = item.getId().replaceAll("::", "#");
			List<String> mIds = item.getMediaIds();
			for(String mId : mIds) {
				
				mId = mId.replaceAll("::", "#");
				
				DBObject doc = new BasicDBObject("id", mId);
				doc.put("reference", ref);
				doc.put("publicationTime", item.getPublicationTime());
				doc.put("userid", uid);
				
				sharesColl.insert(doc);
			}
			

		}
	}
	
	public static void indexToSolr() throws UnknownHostException {
		MongoClient client = new MongoClient("160.40.51.18");
		DB db = client.getDB("Streams");
		DBCollection iColl = db.getCollection("MediaItems");
		
		SolrMediaItemHandler handler = SolrMediaItemHandler.getInstance("http://160.40.51.18:8080/solr/NewsMediaItems");
		
		DBCursor cursor = iColl.find(new BasicDBObject("streamId", new BasicDBObject("$ne", "Twitter")));
		int total = cursor.count();
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k%100==0)
				System.out.println(k + " / " + total + " indexed!");
			DBObject obj = cursor.next();
			String json = obj.toString();
			
			MediaItem mItem = ObjectFactory.createMediaItem(json);
			//System.out.println(mItem.getStreamId());
			handler.insertMediaItem(mItem);
		}
	}
	
	public static void cluster() {
		
	}
}
