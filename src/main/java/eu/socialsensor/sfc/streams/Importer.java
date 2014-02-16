package eu.socialsensor.sfc.streams;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jinstagram.entity.users.basicinfo.UserInfoData;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

import twitter4j.TwitterException;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import com.flickr4java.flickr.people.User;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.abstractions.flickr.FlickrStreamUser;
import eu.socialsensor.framework.abstractions.instagram.InstagramItem;
import eu.socialsensor.framework.abstractions.instagram.InstagramStreamUser;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.StreamUserDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.dao.impl.StreamUserDAOImpl;
import eu.socialsensor.framework.client.mongo.MongoHandler;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.StreamUser;
import eu.socialsensor.framework.common.factories.ObjectFactory;
import eu.socialsensor.framework.retrievers.flickr.FlickrRetriever;
import eu.socialsensor.framework.retrievers.instagram.InstagramRetriever;
import eu.socialsensor.framework.retrievers.twitter.TwitterRetriever;
import eu.socialsensor.framework.streams.StreamException;

public class Importer {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 * @throws StreamException 
	 */
	public static void main(String[] args) throws UnknownHostException, StreamException {
		
		//fixTwitterUser();
		
		//indexToSolr();
		
		//addMediaShares();
		 
		//System.out.println("Fix Stream Users");
		//fixStreamUsers("Flickr");
		
		//System.out.println("Add Media Items");
		//addMediaItems("Instagram");
		
		//System.out.println("Add Stream Users");
		//addStreamUsers("Youtube");
		
		//addMediaShares();
		//System.out.println("Done");
		//System.exit(0);
		
		String instagramToken = "342704836.5b9e1e6.503a35185da54224adaa76161a573e71";
		String instagramSecret = "e53597da6d7749d2a944651bbe6e6f2a";
		
		//String youtubeClientId = "manosetro";
		//String youtubeDevKey = "AI39si6DMfJRhrIFvJRv0qFubHHQypIwjkD-W7tsjLJArVKn9iR-QoT8t-UijtITl4TuyHzK-cxqDDCkCBoJB-seakq1gbt1iQ";
		
		//String flickrKey = "029eab4d06c40e08670d78055bf61205";
		//String flickrSecret = "bc4105126a4dfb8c";
		
		//String facebookKey = "029eab4d06c40e08670d78055bf61205";
		//String facebookSecret = "bc4105126a4dfb8c";
		
		
		//YtMediaRetriever retriever = new YtMediaRetriever(youtubeClientId, youtubeDevKey);
		InstagramRetriever retriever = new InstagramRetriever(instagramSecret, instagramToken, 1, 1);
		//TwitpicMediaRetriever retriever = new TwitpicMediaRetriever();
		//FlickrRetriever retriever = new FlickrRetriever(flickrKey, flickrSecret, 1, 1);
		//FacebookRetriever retriever = new FacebookRetriever(facebookKey, facebookSecret, 1, 1);

		StreamUserDAO dao = new StreamUserDAOImpl("160.40.51.18", "GreekNews", "StreamUsers");
		
		Set<String> ids = new HashSet<String>();
		MongoHandler handler = new MongoHandler("160.40.51.18", "GreekNews", "MediaItems", null);
		List<String> response = handler.findMany(new BasicDBObject("streamId", "Instagram"), 5000);
		System.out.println(response.size());
		for(String json : response) {
			MediaItem mItem = ObjectFactory.createMediaItem(json);
			
			String userId = mItem.getUserId();
			ids.add(userId.split("#")[1]);
		}
		System.out.println(ids.size() + " userids");
		for(String userId : ids) {
			try {
				UserInfoData userResp = retriever.retrieveUser(userId);
				StreamUser user = new InstagramStreamUser(userResp);
				dao.insertStreamUser(user);
				System.out.println(user.toJSONString());
			} catch (InstagramException e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void addMediaItems(String stream) throws UnknownHostException {
		
		//String youtubeClientId = "manosetro";
		//String youtubeDevKey = "AI39si6DMfJRhrIFvJRv0qFubHHQypIwjkD-W7tsjLJArVKn9iR-QoT8t-UijtITl4TuyHzK-cxqDDCkCBoJB-seakq1gbt1iQ";
		String instagramToken = "342704836.5b9e1e6.503a35185da54224adaa76161a573e71";
		String instagramSecret = "e53597da6d7749d2a944651bbe6e6f2a";
		
		//YtMediaRetriever retriever = new YtMediaRetriever(youtubeClientId, youtubeDevKey);
		InstagramRetriever retriever = new InstagramRetriever(instagramSecret, instagramToken, 1, 1);
		
		String mongoHost = "160.40.50.230";
		String mongoDb = "FeteInstagram";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection("MediaItems");
		
		String mongoHost2 = "160.40.51.18";
		String mongoDb2 = "FeteBerlin";
		
		MediaItemDAO dao = new MediaItemDAOImpl(mongoHost2, mongoDb2, "MediaItems");
		//StreamUserDAO udao = new StreamUserDAOImpl(mongoHost2, mongoDb2, "StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", stream);
		
		DBCursor cursor = coll.find(query);
		System.out.println(cursor.count() + " mediaitems found");
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k % 100 == 0)
				System.out.println(k+" media items processed!");
			
			DBObject obj = cursor.next();
			
			String text = (String) obj.get("title");
			text = text + " " + StringUtils.join((List<String>) obj.get("tags"), " ");
			text = text.toLowerCase();
			text = text.replaceAll("\\s+","");
			
			if(!(text.matches(".*fetedelamusique.*") || text.matches(".*fete.*") || text.matches(".*fete14.*") || text.matches(".*feteberlin.*") 
					|| text.matches(".*fete2014.*") || text.matches(".*fete2013.*"))) {
				//System.out.println(obj.get("title"));
				continue;
			}
			
			String mid = obj.get("reference").toString().replaceAll("Instagram::", "");
			MediaFeedData mFeed;
			try {
				mFeed = retriever.retrieveItem(mid);
			} catch (InstagramException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
			if(mFeed!=null) {
				org.jinstagram.entity.common.User u = mFeed.getUser();
				InstagramItem item;
				try {
					item = new InstagramItem(mFeed);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
			
				StreamUser su = new InstagramStreamUser(u);
				if(su!=null) {
					List<MediaItem> mis = item.getMediaItems();
					for(MediaItem mi : mis) {
						System.out.println(mi.toJSONString());
						System.out.println(su.toJSONString());
						
						mi.setUserId(stream+"#"+su.getUserid());
						
						dao.addMediaItem(mi);
						//udao.insertStreamUser(su);
						
					}
				}
			
			}
//			String json = obj.toString();
//			
//			
//			@SuppressWarnings("unchecked")
//			Map<String, Integer> popularity = (Map<String, Integer>) obj.get("popularity");
//			
//			MediaItem mi = ObjectFactory.createMediaItem(json);
//			Integer shares = popularity.get("shares");
//			if(shares==null)
//				shares = 0;
//			Integer likes = popularity.get("likes");
//			if(likes==null)
//				likes = 0;
//			Integer comments = popularity.get("comments");
//			if(comments==null)
//				comments = 0;
//			
//			mi.setShares((long)shares);
//			mi.setLikes((long)likes);
//			mi.setViews(0L);
//			mi.setComments((long)comments);
//			
//			//String uid = obj.get("author").toString().replaceAll("::","#");
//			String uid = stream + "#" + obj.get("author").toString();
//			mi.setUserId(uid);
//			
//			mi.setId(mi.getId().replaceAll("::", "#"));
//			mi.setId(mi.getRef().replaceAll("::", "#"));
//			mi.setRef(mi.getRef().replaceAll("::", "#"));
//			
//			mi.setPageUrl("");
//			
//			System.out.println(mi.toJSONString());
			//dao.addMediaItem(mi);
			
		}
	}
	
	public static void fixMediaItems(String stream) throws UnknownHostException {
		
		String flickrKey = "029eab4d06c40e08670d78055bf61205";
		String flickrSecret = "bc4105126a4dfb8c";
		
		FlickrRetriever retriever = new FlickrRetriever(flickrKey, flickrSecret, 1, 1);
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "FeteBerlin";
	
		StreamUserDAO dao = new StreamUserDAOImpl(mongoHost, mongoDb, "StreamUsers");
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection("StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", stream);
		DBCursor cursor = coll.find(query);
		System.out.println(cursor.count() + " Stream Users found");
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k % 100 == 0)
				System.out.println(k+" Stream Users processed!");

			DBObject obj = cursor.next();
			String json = obj.toString();
			
			
			StreamUser user = ObjectFactory.createUser(json);
			
			User u = retriever.retrieveUser(user.getUserid());
			StreamUser su = new FlickrStreamUser(u);
			
			System.out.println(su.toJSONString());
			
			dao.updateStreamUser(su);
		}
	}

	public static void fixStreamUsers(String stream) throws UnknownHostException {
		
		String flickrKey = "029eab4d06c40e08670d78055bf61205";
		String flickrSecret = "bc4105126a4dfb8c";
		
		FlickrRetriever retriever = new FlickrRetriever(flickrKey, flickrSecret, 1, 1);
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "FeteBerlin";
	
		StreamUserDAO dao = new StreamUserDAOImpl(mongoHost, mongoDb, "StreamUsers");
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection("StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", stream);
		DBCursor cursor = coll.find(query);
		System.out.println(cursor.count() + " Stream Users found");
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k % 100 == 0)
				System.out.println(k+" Stream Users processed!");

			DBObject obj = cursor.next();
			String json = obj.toString();
			
			
			StreamUser user = ObjectFactory.createUser(json);
			
			User u = retriever.retrieveUser(user.getUserid());
			StreamUser su = new FlickrStreamUser(u);
			
			System.out.println(su.toJSONString());
			
			dao.updateStreamUser(su);
		}
	}

	public static void addStreamUsers(String stream) throws UnknownHostException {
		
		String mongoHost = "160.40.50.230";
		String mongoDb = "FeteYoutube";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection("StreamUsers");
		
		String mongoHost2 = "160.40.51.18";
		String mongoDb2 = "FeteBerlin";
		
		StreamUserDAO dao = new StreamUserDAOImpl(mongoHost2, mongoDb2, "StreamUsers");
		
		BasicDBObject query = new BasicDBObject("streamId", stream);
		DBCursor cursor = coll.find(query);
		System.out.println(cursor.count() + " Stream Users found");
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k % 100 == 0)
				System.out.println(k+" Stream Users processed!");

			DBObject obj = cursor.next();
			String json = obj.toString();
			
			
			StreamUser user = ObjectFactory.createUser(json);
			
			@SuppressWarnings("unchecked")
			Map<String, Integer> popularity = (Map<String, Integer>) obj.get("popularity");
			if(popularity!=null) {
				Integer followers = popularity.get("followers");
				if(followers==null)
					followers = 0;
				Integer friends = popularity.get("friends");
				if(friends==null)
					friends = 0;
			
				user.setFollowers((long)followers);
				user.setFriends((long)friends);
			}
			
			user.setId(user.getId().replaceAll("::", "#"));
			
			String pageUrl = "https://www.facebook.com/"+user.getUserid();
			user.setPageUrl(pageUrl);
			user.setUsername(user.getUserid());
			user.setProfileImage("http://graph.facebook.com/" + user.getUserid() + "/picture");
			
			System.out.println(user.toJSONString());
			dao.insertStreamUser(user);
			
		}
	}

	public static void fixClusters() throws UnknownHostException {
		String mongoHost = "";
		String mongoDb = "";
		String mongoCollection = "MediaItemClusters";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection(mongoCollection);
	
		DBCursor cursor = coll.find();
		System.out.println(cursor.count() + " clusters found");
		int k=0;
		while(cursor.hasNext()) {
			
			k++;
			if(k % 1000 == 0)
				System.out.println(k+" clusters processed!");
			
			DBObject obj = cursor.next();
			
			String id = (String) obj.get("id");
			Integer count = (Integer) obj.get("count");
			@SuppressWarnings("unchecked")
			List<String> members = (List<String>) obj.get("members");
			
			if(count != members.size()) {
				BasicDBObject query = new BasicDBObject("id",id);
					
				DBObject doc = new BasicDBObject("$set", new BasicDBObject("count", members.size()));
				coll.update(query, doc);
			}
		}
	}

	public static void fixWPTime() throws UnknownHostException {
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "gezi";
		String mongoCollection = "MediaItems";
		//String usersCollection = "WebPages";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection mediaColl = db.getCollection(mongoCollection);
		//DBCollection wpColl = db.getCollection(usersCollection);

		
		DBCursor cursor = mediaColl.find(new BasicDBObject("streamId","Web"));
		System.out.println(cursor.count() + " Twitter users found!");
		int k=0;

		while(cursor.hasNext()) {
			
			k++;
			if(k % 1000 == 0)
				System.out.println(k+" clusters processed!");
			
			//DBObject obj = cursor.next();
			
			//Object ref = obj.get("reference");
			
			//DBObject wp = wpColl.findOne(new BasicDBObject("reference", ref));

			
		}
	}
	
	public static void fixTwitterUser() throws UnknownHostException, StreamException {
		
		
		String oAuthConsumerKey 		= 	"YZdoz58cjYg8sCyIGGec3A";
		String oAuthConsumerSecret 		= 	"xAMpmtDdGkRZRVeR5saoZpbxbdtG3VoTxpWfHOqM";
		String oAuthAccessToken 		= 	"204974667-TmEQ0NztWqxfXXVO8HPSUDPtqoXfw99c8Yu0ijEJ";
		String oAuthAccessTokenSecret 	= 	"bRtzNKYi8ocJ1DGFMx3mtWdXQxtVeX6vZWGuKuWAT0";
		
		if (oAuthConsumerKey == null || oAuthConsumerSecret == null ||
				oAuthAccessToken == null || oAuthAccessTokenSecret == null) {
			throw new StreamException("Stream requires authentication");
		}
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true)
			.setOAuthConsumerKey(oAuthConsumerKey)
			.setOAuthConsumerSecret(oAuthConsumerSecret)
			.setOAuthAccessToken(oAuthAccessToken)
			.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
		Configuration conf = cb.build();
		
		TwitterRetriever retriever = new TwitterRetriever(conf);
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "gezi";
		String mongoCollection = "MediaItems";
		String usersCollection = "StreamUsers";
		
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection mediaColl = db.getCollection(mongoCollection);
		DBCollection usersColl = db.getCollection(usersCollection);
		
		Set<String> userids = new HashSet<String>();
		
		DBCursor cursor = mediaColl.find(new BasicDBObject("streamId","Twitter"));
		System.out.println(cursor.count() + " Twitter users found!");
		int k=0;
		long[] ids = new long[100];
		int i=0;
		while(cursor.hasNext()) {
			
			k++;
			if(k % 1000 == 0)
				System.out.println(k+" clusters processed!");
			
			DBObject obj = cursor.next();
			
			String uid = (String) obj.get("uid");
			if(userids.contains(uid))
				continue;
			
			userids.add(uid);
			//System.out.println(uid);
			String userid = uid.replaceAll("Twitter#", "");
			ids[i++] = Long.parseLong(userid); 
					
			if(i==100) {
				i = 0;
				try {
					Map<String, String> map = retriever.retrieveUserPictures(ids);
					for(Entry<String, String> e : map.entrySet()) {
						BasicDBObject query = new BasicDBObject("id", e.getKey());
						
						DBObject doc = new BasicDBObject("$set", new BasicDBObject("profileImage", e.getValue()));
						System.out.println(query.toString());
						System.out.println(doc.toString());
						System.out.println("================================");
						usersColl.update(query, doc, false, true);
					}
				} catch (TwitterException e) {
					e.printStackTrace();
				}
			}
			
			
			
			
		}
		System.out.println(userids.size() + " unique Twitter users!");
	}
	
	public static void addMediaShares() throws UnknownHostException {
		
		String mongoHost1 = "160.40.50.207";
		String mongoDb1 = "FeteBerlin";
		String mongoCollection1 = "Items";
		
		String mongoHost2 = "160.40.51.18";
		String mongoDb2 = "FeteBerlin";
		String mongoCollection2 = "MediaShares";
		
		MongoClient client = new MongoClient(mongoHost1);
		DB db = client.getDB(mongoDb1);
		DBCollection iColl = db.getCollection(mongoCollection1);
		
		MongoClient client2 = new MongoClient(mongoHost2);
		DB db2 = client2.getDB(mongoDb2);
		DBCollection sharesColl = db2.getCollection(mongoCollection2);

		
		DBCursor cursor = iColl.find(new BasicDBObject("id", new BasicDBObject("$exists", true)));
		System.out.println(cursor.count() + " items found");
		int k=0;
		while(cursor.hasNext()) {
			
			k++;
			if(k % 1000 == 0)
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
		
		String mongoHost = "160.40.51.18";
		String mongoDb = "FeteBerlin";
		String mongoCollection = "MediaItems";
		
		String solrService = "http://160.40.51.18:8080/solr/FeteMediaItems";
		
		MongoClient client = new MongoClient(mongoHost);
		DB db = client.getDB(mongoDb);
		DBCollection coll = db.getCollection(mongoCollection);
		
		SolrMediaItemHandler handler = SolrMediaItemHandler.getInstance(solrService);
		
		DBCursor cursor = coll.find();
		int total = cursor.count();
		int k=0;
		while(cursor.hasNext()) {
			k++;
			if(k % 100 == 0)
				System.out.println(k + " / " + total + " indexed!");
			
			DBObject obj = cursor.next();
			
			String json = obj.toString();
			MediaItem mItem = ObjectFactory.createMediaItem(json);
			
			handler.insertMediaItem(mItem);
		}
	}
	
	
}
