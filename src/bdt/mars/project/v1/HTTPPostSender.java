package bdt.mars.project.v1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONObject;

public class HTTPPostSender {
	public static void saveToES(String args[]) {
		String who = args[0];
		String voteFor = args[1];
		String count = args[2];
		String timestamp = args[3];
		String restUrl = "https://c8d08836a88346e6894f02cc722ed09a.us-central1.gcp.cloud.es.io:9243/election/_doc/"
				+ who+"_" + voteFor;
		String username = "elastic";
		String password = "QgEzVuUrUePRILVY1Zl59kJ6";
		JSONObject user = new JSONObject();
		user.put("voteFor", voteFor);
		user.put("count", count);
		user.put("timestamp", timestamp);
		String jsonData = user.toString();
		HTTPPostSender httpPostReq = new HTTPPostSender();
		HttpPost httpPost = httpPostReq.createConnectivity(restUrl, username,
				password);
		httpPostReq.executeReq(jsonData, httpPost);
	}

	HttpPost createConnectivity(String restUrl, String username, String password) {
		HttpPost post = new HttpPost(restUrl);
		String auth = new StringBuffer(username).append(":").append(password)
				.toString();
		byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset
				.forName("US-ASCII")));
		String authHeader = "Basic " + new String(encodedAuth);
		post.setHeader("AUTHORIZATION", authHeader);
		post.setHeader("Content-Type", "application/json");
		post.setHeader("Accept", "application/json");
		post.setHeader("X-Stream", "true");
		return post;
	}

	void executeReq(String jsonData, HttpPost httpPost) {
		try {
			executeHttpRequest(jsonData, httpPost);
		} catch (UnsupportedEncodingException e) {
			System.out.println("error while encoding api url : " + e);
		} catch (IOException e) {
			System.out
					.println("ioException occured while sending http request : "
							+ e);
		} catch (Exception e) {
			System.out
					.println("exception occured while sending http request : "
							+ e);
		} finally {
			httpPost.releaseConnection();
		}
	}

	void executeHttpRequest(String jsonData, HttpPost httpPost)
			throws UnsupportedEncodingException, IOException {
		HttpResponse response = null;
		String line = "";
		StringBuffer result = new StringBuffer();
		httpPost.setEntity(new StringEntity(jsonData));
		HttpClient client = HttpClientBuilder.create().build();
		response = client.execute(httpPost);
		//System.out.println("Post parameters : " + jsonData);
		//System.out.println("Response Code : "				+ response.getStatusLine().getStatusCode());
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				response.getEntity().getContent()));
		while ((line = reader.readLine()) != null) {
			result.append(line);
		}
		//System.out.println(result.toString());
	}
}