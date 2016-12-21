package com.cealiy.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;


public class HttpFSFileSystemFactory {
	
	private String user;
	
	private String host;
	
	private String port;
	
	
	
	public HttpFSFileSystem get(){
		HttpFSFileSystem fs=null;
		try {
			URI temp=new URI("webhdfs://"+this.host+":"+this.port);
			fs=new HttpFSFileSystem();
			fs.setHODOOP_HTTPFS_USER(this.user);
			Configuration conf=new Configuration();
			fs.initialize(temp,conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}
	
	
	public static HttpFSFileSystem get(String uri,String user){
		HttpFSFileSystem fs=null;
		try {
			URI temp=new URI(uri);
			fs=new HttpFSFileSystem();
			fs.setHODOOP_HTTPFS_USER(user);
			Configuration conf=new Configuration();
			fs.initialize(temp,conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}

	public String getHdfsUri(){
		return "webhdfs://"+this.host+":"+this.port;
	}

	public String getUser() {
		return user;
	}


	public String getHost() {
		return host;
	}


	public void setHost(String host) {
		this.host = host;
	}


	public String getPort() {
		return port;
	}


	public void setPort(String port) {
		this.port = port;
	}


	public void setUser(String user) {
		this.user = user;
	}


	

}
