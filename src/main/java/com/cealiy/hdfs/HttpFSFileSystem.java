/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package com.cealiy.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.User;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.DelegationTokenRenewer.Renewable;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.AclStatus.Builder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@Private
@SuppressWarnings({"unused","unchecked","rawtypes"})
public class HttpFSFileSystem extends FileSystem implements Renewable {
	public static final String SERVICE_NAME = "/webhdfs";
	public static final String SERVICE_VERSION = "/v1";
	public static final String SCHEME = "webhdfs";
	public static final String OP_PARAM = "op";
	public static final String DO_AS_PARAM = "doas";
	public static final String OVERWRITE_PARAM = "overwrite";
	public static final String REPLICATION_PARAM = "replication";
	public static final String BLOCKSIZE_PARAM = "blocksize";
	public static final String PERMISSION_PARAM = "permission";
	public static final String ACLSPEC_PARAM = "aclspec";
	public static final String DESTINATION_PARAM = "destination";
	public static final String RECURSIVE_PARAM = "recursive";
	public static final String SOURCES_PARAM = "sources";
	public static final String OWNER_PARAM = "owner";
	public static final String GROUP_PARAM = "group";
	public static final String MODIFICATION_TIME_PARAM = "modificationtime";
	public static final String ACCESS_TIME_PARAM = "accesstime";
	public static final String XATTR_NAME_PARAM = "xattr.name";
	public static final String XATTR_VALUE_PARAM = "xattr.value";
	public static final String XATTR_SET_FLAG_PARAM = "flag";
	public static final String XATTR_ENCODING_PARAM = "encoding";
	public static final String NEW_LENGTH_PARAM = "newlength";
	public static final Short DEFAULT_PERMISSION = Short.valueOf((short) 493);
	public static final String ACLSPEC_DEFAULT = "";
	public static final String RENAME_JSON = "boolean";
	public static final String TRUNCATE_JSON = "boolean";
	public static final String DELETE_JSON = "boolean";
	public static final String MKDIRS_JSON = "boolean";
	public static final String HOME_DIR_JSON = "Path";
	public static final String SET_REPLICATION_JSON = "boolean";
	public static final String UPLOAD_CONTENT_TYPE = "application/octet-stream";
	public static final String FILE_STATUSES_JSON = "FileStatuses";
	public static final String FILE_STATUS_JSON = "FileStatus";
	public static final String PATH_SUFFIX_JSON = "pathSuffix";
	public static final String TYPE_JSON = "type";
	public static final String LENGTH_JSON = "length";
	public static final String OWNER_JSON = "owner";
	public static final String GROUP_JSON = "group";
	public static final String PERMISSION_JSON = "permission";
	public static final String ACCESS_TIME_JSON = "accessTime";
	public static final String MODIFICATION_TIME_JSON = "modificationTime";
	public static final String BLOCK_SIZE_JSON = "blockSize";
	public static final String REPLICATION_JSON = "replication";
	public static final String XATTRS_JSON = "XAttrs";
	public static final String XATTR_NAME_JSON = "name";
	public static final String XATTR_VALUE_JSON = "value";
	public static final String XATTRNAMES_JSON = "XAttrNames";
	public static final String FILE_CHECKSUM_JSON = "FileChecksum";
	public static final String CHECKSUM_ALGORITHM_JSON = "algorithm";
	public static final String CHECKSUM_BYTES_JSON = "bytes";
	public static final String CHECKSUM_LENGTH_JSON = "length";
	public static final String CONTENT_SUMMARY_JSON = "ContentSummary";
	public static final String CONTENT_SUMMARY_DIRECTORY_COUNT_JSON = "directoryCount";
	public static final String CONTENT_SUMMARY_FILE_COUNT_JSON = "fileCount";
	public static final String CONTENT_SUMMARY_LENGTH_JSON = "length";
	public static final String CONTENT_SUMMARY_QUOTA_JSON = "quota";
	public static final String CONTENT_SUMMARY_SPACE_CONSUMED_JSON = "spaceConsumed";
	public static final String CONTENT_SUMMARY_SPACE_QUOTA_JSON = "spaceQuota";
	public static final String ACL_STATUS_JSON = "AclStatus";
	public static final String ACL_STICKY_BIT_JSON = "stickyBit";
	public static final String ACL_ENTRIES_JSON = "entries";
	public static final String ACL_BIT_JSON = "aclBit";
	public static final int HTTP_TEMPORARY_REDIRECT = 307;
	private static final String HTTP_GET = "GET";
	private static final String HTTP_PUT = "PUT";
	private static final String HTTP_POST = "POST";
	private static final String HTTP_DELETE = "DELETE";
	private DelegationTokenAuthenticatedURL authURL;
	private Token authToken = new Token();
	private URI uri;
	private Path workingDir;
	private String HODOOP_HTTPFS_USER;
	private String USER_QUERY_NAME="user.name";

	private HttpURLConnection getConnection(String method, Map<String, String> params, Path path, boolean makeQualified)
			throws IOException {
		return this.getConnection(method, params, (Map) null, path, makeQualified);
	}

	private HttpURLConnection getConnection(final String method, Map<String, String> params,
			Map<String, List<String>> multiValuedParams, Path path, boolean makeQualified) throws IOException {
		
		params.put(this.USER_QUERY_NAME,this.HODOOP_HTTPFS_USER);
		final URL url = HttpFSUtils.createURL(path, params, multiValuedParams);
		HttpURLConnection ex =(HttpURLConnection) url.openConnection();
		ex.setRequestMethod(method);
		if (method.equals("POST") || method.equals("PUT")) {
			ex.setDoOutput(true);
		}
		return ex;
	}


	public void initialize(URI name, Configuration conf) throws IOException {
		this.setConf(conf);
		super.initialize(name, conf);
		try {
			this.uri = new URI(name.getScheme() + "://" + name.getAuthority());
		} catch (URISyntaxException arg5) {
			throw new IOException(arg5);
		}
		Class klass = this.getConf().getClass("httpfs.authenticator.class", KerberosDelegationTokenAuthenticator.class,
				DelegationTokenAuthenticator.class);
		DelegationTokenAuthenticator authenticator = (DelegationTokenAuthenticator) ReflectionUtils.newInstance(klass,
				this.getConf());
		this.authURL = new DelegationTokenAuthenticatedURL(authenticator);
	}

	
	public String getScheme() {
		return "webhdfs";
	}

	public URI getUri() {
		return this.uri;
	}

	protected int getDefaultPort() {
		return this.getConf().getInt("dfs.http.port", 14000);
	}

	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.OPEN.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.OPEN.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		return new FSDataInputStream(new HttpFSFileSystem.HttpFSDataInputStream(conn.getInputStream(), bufferSize));
	}

	public static String permissionToString(FsPermission p) {
		return Integer.toString(p == null ? DEFAULT_PERMISSION.shortValue() : p.toShort(), 8);
	}

	private FSDataOutputStream uploadData(String method, Path f, Map<String, String> params, int bufferSize,
			int expectedStatus) throws IOException {
		HttpURLConnection conn = this.getConnection(method, params, f, true);
		conn.setInstanceFollowRedirects(false);
		boolean exceptionAlreadyHandled = false;
		try {
			if (conn.getResponseCode() == 307) {
				exceptionAlreadyHandled = true;
				String ex = conn.getHeaderField("Location");
				if (ex != null) {
					conn = this.getConnection(ex,method);
					conn.setRequestProperty("Content-Type", "application/octet-stream");

					try {
						BufferedOutputStream ex1 = new BufferedOutputStream(conn.getOutputStream(), bufferSize);
						return new HttpFSFileSystem.HttpFSDataOutputStream(conn, ex1, expectedStatus, this.statistics);
					} catch (IOException arg9) {
						HttpExceptionUtils.validateResponse(conn, expectedStatus);
						throw arg9;
					}
				} else {
					HttpExceptionUtils.validateResponse(conn, 307);
					throw new IOException("Missing HTTP \'Location\' header for [" + conn.getURL() + "]");
				}
			} else {
				throw new IOException(MessageFormat.format("Expected HTTP status was [307], received [{0}]",
						new Object[] { Integer.valueOf(conn.getResponseCode()) }));
			}
		} catch (IOException arg10) {
			if (exceptionAlreadyHandled) {
				throw arg10;
			} else {
				HttpExceptionUtils.validateResponse(conn, 307);
				throw arg10;
			}
		}
	}

	private HttpURLConnection getConnection(String path, String method) throws IOException {
		if(path==null||path.isEmpty()){
			return null;
		}
		if(path.contains("?")){
			path=path+"&"+this.USER_QUERY_NAME+"="+this.HODOOP_HTTPFS_USER;
		}else{
			path=path+"?"+this.USER_QUERY_NAME+"="+this.HODOOP_HTTPFS_USER;
		}
		URL url=new URL(path);
		HttpURLConnection ex =(HttpURLConnection) url.openConnection();
		ex.setRequestMethod(method);
		if (method.equals("POST") || method.equals("PUT")) {
			ex.setDoOutput(true);
		}
		return ex;
		
	}

	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.CREATE.toString());
		params.put("overwrite", Boolean.toString(overwrite));
		params.put("replication", Short.toString(replication));
		params.put("blocksize", Long.toString(blockSize));
		params.put("permission", permissionToString(permission));
		return this.uploadData(HttpFSFileSystem.Operation.CREATE.getMethod(), f, params, bufferSize, 201);
	}

	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.APPEND.toString());
		return this.uploadData(HttpFSFileSystem.Operation.APPEND.getMethod(), f, params, bufferSize, 200);
	}

	public boolean truncate(Path f, long newLength) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.TRUNCATE.toString());
		params.put("newlength", Long.toString(newLength));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.TRUNCATE.getMethod(), params, f, true);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return ((Boolean) json.get("boolean")).booleanValue();
	}

	public void concat(Path f, Path[] psrcs) throws IOException {
		ArrayList strPaths = new ArrayList(psrcs.length);
		Path[] srcs = psrcs;
		int params = psrcs.length;

		for (int conn = 0; conn < params; ++conn) {
			Path psrc = srcs[conn];
			strPaths.add(psrc.toUri().getPath());
		}

		String arg7 = StringUtils.join(",", strPaths);
		HashMap arg8 = new HashMap();
		arg8.put("op", HttpFSFileSystem.Operation.CONCAT.toString());
		arg8.put("sources", arg7);
		HttpURLConnection arg9 = this.getConnection(HttpFSFileSystem.Operation.CONCAT.getMethod(), arg8, f, true);
		HttpExceptionUtils.validateResponse(arg9, 200);
	}

	public boolean rename(Path src, Path dst) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.RENAME.toString());
		params.put("destination", dst.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.RENAME.getMethod(), params, src, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return ((Boolean) json.get("boolean")).booleanValue();
	}

	@Deprecated
	public boolean delete(Path f) throws IOException {
		return this.delete(f, false);
	}

	public boolean delete(Path f, boolean recursive) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.DELETE.toString());
		params.put("recursive", Boolean.toString(recursive));
		params.put("user.name","root");
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.DELETE.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return ((Boolean) json.get("boolean")).booleanValue();
	}

	public FileStatus[] listStatus(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.LISTSTATUS.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.LISTSTATUS.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		json = (JSONObject) json.get("FileStatuses");
		JSONArray jsonArray = (JSONArray) json.get("FileStatus");
		FileStatus[] array = new FileStatus[jsonArray.size()];
		f = this.makeQualified(f);

		for (int i = 0; i < jsonArray.size(); ++i) {
			array[i] = this.createFileStatus(f, (JSONObject) jsonArray.get(i));
		}

		return array;
	}

	public void setWorkingDirectory(Path newDir) {
		this.workingDir = newDir;
	}

	public Path getWorkingDirectory() {
		if (this.workingDir == null) {
			this.workingDir = this.getHomeDirectory();
		}

		return this.workingDir;
	}

	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.MKDIRS.toString());
		params.put("permission", permissionToString(permission));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.MKDIRS.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return ((Boolean) json.get("boolean")).booleanValue();
	}

	public FileStatus getFileStatus(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETFILESTATUS.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETFILESTATUS.getMethod(), params, f,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		json = (JSONObject) json.get("FileStatus");
		f = this.makeQualified(f);
		return this.createFileStatus(f, json);
	}

	public Path getHomeDirectory() {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETHOMEDIRECTORY.toString());

		try {
			HttpURLConnection ex = this.getConnection(HttpFSFileSystem.Operation.GETHOMEDIRECTORY.getMethod(), params,
					new Path(this.getUri().toString(), "/"), false);
			HttpExceptionUtils.validateResponse(ex, 200);
			JSONObject json = (JSONObject) HttpFSUtils.jsonParse(ex);
			return new Path((String) json.get("Path"));
		} catch (IOException arg3) {
			throw new RuntimeException(arg3);
		}
	}

	public void setOwner(Path p, String username, String groupname) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETOWNER.toString());
		params.put("owner", username);
		params.put("group", groupname);
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETOWNER.getMethod(), params, p, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void setPermission(Path p, FsPermission permission) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETPERMISSION.toString());
		params.put("permission", permissionToString(permission));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETPERMISSION.getMethod(), params, p,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void setTimes(Path p, long mtime, long atime) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETTIMES.toString());
		params.put("modificationtime", Long.toString(mtime));
		params.put("accesstime", Long.toString(atime));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETTIMES.getMethod(), params, p, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public boolean setReplication(Path src, short replication) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETREPLICATION.toString());
		params.put("replication", Short.toString(replication));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETREPLICATION.getMethod(), params, src,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return ((Boolean) json.get("boolean")).booleanValue();
	}

	public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.MODIFYACLENTRIES.toString());
		params.put("aclspec", AclEntry.aclSpecToString(aclSpec));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.MODIFYACLENTRIES.getMethod(), params,
				path, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.REMOVEACLENTRIES.toString());
		params.put("aclspec", AclEntry.aclSpecToString(aclSpec));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.REMOVEACLENTRIES.getMethod(), params,
				path, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void removeDefaultAcl(Path path) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.REMOVEDEFAULTACL.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.REMOVEDEFAULTACL.getMethod(), params,
				path, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void removeAcl(Path path) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.REMOVEACL.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.REMOVEACL.getMethod(), params, path,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETACL.toString());
		params.put("aclspec", AclEntry.aclSpecToString(aclSpec));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETACL.getMethod(), params, path, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public AclStatus getAclStatus(Path path) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETACLSTATUS.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETACLSTATUS.getMethod(), params, path,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		json = (JSONObject) json.get("AclStatus");
		return this.createAclStatus(json);
	}

	private FileStatus createFileStatus(Path parent, JSONObject json) {
		String pathSuffix = (String) json.get("pathSuffix");
		Path path = pathSuffix.equals("") ? parent : new Path(parent, pathSuffix);
		HttpFSFileSystem.FILE_TYPE type = HttpFSFileSystem.FILE_TYPE.valueOf((String) json.get("type"));
		long len = ((Long) json.get("length")).longValue();
		String owner = (String) json.get("owner");
		String group = (String) json.get("group");
		FsPermission permission = new FsPermission(Short.parseShort((String) json.get("permission"), 8));
		long aTime = ((Long) json.get("accessTime")).longValue();
		long mTime = ((Long) json.get("modificationTime")).longValue();
		long blockSize = ((Long) json.get("blockSize")).longValue();
		short replication = ((Long) json.get("replication")).shortValue();
		FileStatus fileStatus = null;
		switch (type.ordinal()) 
		{
		default:
			fileStatus = new FileStatus(len, type == HttpFSFileSystem.FILE_TYPE.DIRECTORY, replication, blockSize,
					mTime, aTime, permission, owner, group, path);
			break;
		case 2:
			Object symLink = null;
			fileStatus = new FileStatus(len, false, replication, blockSize, mTime, aTime, permission, owner, group,
					(Path) symLink, path);
			break;
		}

		return fileStatus;
	}

	private AclStatus createAclStatus(JSONObject json) {
		Builder aclStatusBuilder = (new Builder()).owner((String) json.get("owner")).group((String) json.get("group"))
				.stickyBit(((Boolean) json.get("stickyBit")).booleanValue());
		JSONArray entries = (JSONArray) json.get("entries");
		Iterator arg3 = entries.iterator();

		while (arg3.hasNext()) {
			Object e = arg3.next();
			aclStatusBuilder.addEntry(AclEntry.parseAclEntry(e.toString(), true));
		}

		return aclStatusBuilder.build();
	}

	public ContentSummary getContentSummary(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETCONTENTSUMMARY.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETCONTENTSUMMARY.getMethod(), params, f,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) ((JSONObject) HttpFSUtils.jsonParse(conn)).get("ContentSummary");
		return (new org.apache.hadoop.fs.ContentSummary.Builder()).length(((Long) json.get("length")).longValue())
				.fileCount(((Long) json.get("fileCount")).longValue())
				.directoryCount(((Long) json.get("directoryCount")).longValue())
				.quota(((Long) json.get("quota")).longValue())
				.spaceConsumed(((Long) json.get("spaceConsumed")).longValue())
				.spaceQuota(((Long) json.get("spaceQuota")).longValue()).build();
	}

	public FileChecksum getFileChecksum(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETFILECHECKSUM.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETFILECHECKSUM.getMethod(), params, f,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
		final JSONObject json = (JSONObject) ((JSONObject) HttpFSUtils.jsonParse(conn)).get("FileChecksum");
		return new FileChecksum() {
			public String getAlgorithmName() {
				return (String) json.get("algorithm");
			}

			public int getLength() {
				return ((Long) json.get("length")).intValue();
			}

			public byte[] getBytes() {
				return StringUtils.hexStringToByte((String) json.get("bytes"));
			}

			public void write(DataOutput out) throws IOException {
				throw new UnsupportedOperationException();
			}

			public void readFields(DataInput in) throws IOException {
				throw new UnsupportedOperationException();
			}
		};
	}

	public org.apache.hadoop.security.token.Token<?> getDelegationToken(final String renewer) throws IOException {
		try {
			return (org.apache.hadoop.security.token.Token) UserGroupInformation.getCurrentUser()
					.doAs(new PrivilegedExceptionAction() {
						public org.apache.hadoop.security.token.Token<?> run() throws Exception {
							return HttpFSFileSystem.this.authURL.getDelegationToken(HttpFSFileSystem.this.uri.toURL(),
									HttpFSFileSystem.this.authToken, renewer);
						}
					});
		} catch (Exception arg2) {
			if (arg2 instanceof IOException) {
				throw (IOException) arg2;
			} else {
				throw new IOException(arg2);
			}
		}
	}

	public long renewDelegationToken(org.apache.hadoop.security.token.Token<?> token) throws IOException {
		try {
			return ((Long) UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction() {
				public Long run() throws Exception {
					return Long.valueOf(HttpFSFileSystem.this.authURL
							.renewDelegationToken(HttpFSFileSystem.this.uri.toURL(), HttpFSFileSystem.this.authToken));
				}
			})).longValue();
		} catch (Exception arg2) {
			if (arg2 instanceof IOException) {
				throw (IOException) arg2;
			} else {
				throw new IOException(arg2);
			}
		}
	}

	public void cancelDelegationToken(org.apache.hadoop.security.token.Token<?> token) throws IOException {
		this.authURL.cancelDelegationToken(this.uri.toURL(), this.authToken);
	}

	public org.apache.hadoop.security.token.Token<?> getRenewToken() {
		return null;
	}

	public <T extends TokenIdentifier> void setDelegationToken(org.apache.hadoop.security.token.Token<T> token) {
	}

	public void setXAttr(Path f, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.SETXATTR.toString());
		params.put("xattr.name", name);
		if (value != null) {
			params.put("xattr.value", XAttrCodec.encodeValue(value, XAttrCodec.HEX));
		}

		params.put("flag", EnumSetParam.toString(flag));
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.SETXATTR.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	public byte[] getXAttr(Path f, String name) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETXATTRS.toString());
		params.put("xattr.name", name);
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETXATTRS.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		Map xAttrs = this.createXAttrMap((JSONArray) json.get("XAttrs"));
		return xAttrs != null ? (byte[]) xAttrs.get(name) : null;
	}

	private Map<String, byte[]> createXAttrMap(JSONArray jsonArray) throws IOException {
		HashMap xAttrs = Maps.newHashMap();
		Iterator arg2 = jsonArray.iterator();

		while (arg2.hasNext()) {
			Object obj = arg2.next();
			JSONObject jsonObj = (JSONObject) obj;
			String name = (String) jsonObj.get("name");
			byte[] value = XAttrCodec.decodeValue((String) jsonObj.get("value"));
			xAttrs.put(name, value);
		}

		return xAttrs;
	}

	private List<String> createXAttrNames(String xattrNamesStr) throws IOException {
		JSONParser parser = new JSONParser();

		try {
			JSONArray jsonArray = (JSONArray) parser.parse(xattrNamesStr);
			ArrayList e = Lists.newArrayListWithCapacity(jsonArray.size());
			Iterator arg4 = jsonArray.iterator();

			while (arg4.hasNext()) {
				Object name = arg4.next();
				e.add((String) name);
			}

			return e;
		} catch (ParseException arg6) {
			throw new IOException("JSON parser error, " + arg6.getMessage(), arg6);
		}
	}

	public Map<String, byte[]> getXAttrs(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETXATTRS.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETXATTRS.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return this.createXAttrMap((JSONArray) json.get("XAttrs"));
	}

	public Map<String, byte[]> getXAttrs(Path f, List<String> names) throws IOException {
		Preconditions.checkArgument(names != null && !names.isEmpty(), "XAttr names cannot be null or empty.");
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.GETXATTRS.toString());
		HashMap multiValuedParams = Maps.newHashMap();
		multiValuedParams.put("xattr.name", names);
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.GETXATTRS.getMethod(), params,
				multiValuedParams, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return this.createXAttrMap((JSONArray) json.get("XAttrs"));
	}

	public List<String> listXAttrs(Path f) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.LISTXATTRS.toString());
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.LISTXATTRS.getMethod(), params, f, true);
		HttpExceptionUtils.validateResponse(conn, 200);
		JSONObject json = (JSONObject) HttpFSUtils.jsonParse(conn);
		return this.createXAttrNames((String) json.get("XAttrNames"));
	}
	
	

	public String getHODOOP_HTTPFS_USER() {
		return HODOOP_HTTPFS_USER;
	}

	public void setHODOOP_HTTPFS_USER(String hODOOP_HTTPFS_USER) {
		HODOOP_HTTPFS_USER = hODOOP_HTTPFS_USER;
	}

	public void removeXAttr(Path f, String name) throws IOException {
		HashMap params = new HashMap();
		params.put("op", HttpFSFileSystem.Operation.REMOVEXATTR.toString());
		params.put("xattr.name", name);
		HttpURLConnection conn = this.getConnection(HttpFSFileSystem.Operation.REMOVEXATTR.getMethod(), params, f,
				true);
		HttpExceptionUtils.validateResponse(conn, 200);
	}

	private static class HttpFSDataOutputStream extends FSDataOutputStream {
		private HttpURLConnection conn;
		private int closeStatus;

		public HttpFSDataOutputStream(HttpURLConnection conn, OutputStream out, int closeStatus, Statistics stats)
				throws IOException {
			super(out, stats);
			this.conn = conn;
			this.closeStatus = closeStatus;
		}

		public void close() throws IOException {
			try {
				super.close();
			} finally {
				HttpExceptionUtils.validateResponse(this.conn, this.closeStatus);
			}

		}
	}

	private static class HttpFSDataInputStream extends FilterInputStream implements Seekable, PositionedReadable {
		protected HttpFSDataInputStream(InputStream in, int bufferSize) {
			super(new BufferedInputStream(in, bufferSize));
		}

		public int read(long position, byte[] buffer, int offset, int length) throws IOException {
			throw new UnsupportedOperationException();
		}

		public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
			throw new UnsupportedOperationException();
		}

		public void readFully(long position, byte[] buffer) throws IOException {
			throw new UnsupportedOperationException();
		}

		public void seek(long pos) throws IOException {
			throw new UnsupportedOperationException();
		}

		public long getPos() throws IOException {
			throw new UnsupportedOperationException();
		}

		public boolean seekToNewSource(long targetPos) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	@Private
	public static enum Operation {
		OPEN("GET"), GETFILESTATUS("GET"), LISTSTATUS("GET"), GETHOMEDIRECTORY("GET"), GETCONTENTSUMMARY(
				"GET"), GETFILECHECKSUM("GET"), GETFILEBLOCKLOCATIONS("GET"), INSTRUMENTATION("GET"), GETACLSTATUS(
						"GET"), APPEND("POST"), CONCAT("POST"), TRUNCATE("POST"), CREATE("PUT"), MKDIRS("PUT"), RENAME(
								"PUT"), SETOWNER("PUT"), SETPERMISSION("PUT"), SETREPLICATION("PUT"), SETTIMES(
										"PUT"), MODIFYACLENTRIES("PUT"), REMOVEACLENTRIES("PUT"), REMOVEDEFAULTACL(
												"PUT"), REMOVEACL("PUT"), SETACL("PUT"), DELETE("DELETE"), SETXATTR(
														"PUT"), GETXATTRS("GET"), REMOVEXATTR("PUT"), LISTXATTRS("GET");

		private String httpMethod;

		private Operation(String httpMethod) {
			this.httpMethod = httpMethod;
		}

		public String getMethod() {
			return this.httpMethod;
		}
	}

	public static enum FILE_TYPE {
		FILE, DIRECTORY, SYMLINK;

		public static HttpFSFileSystem.FILE_TYPE getType(FileStatus fileStatus) {
			if (fileStatus.isFile()) {
				return FILE;
			} else if (fileStatus.isDirectory()) {
				return DIRECTORY;
			} else if (fileStatus.isSymlink()) {
				return SYMLINK;
			} else {
				throw new IllegalArgumentException("Could not determine filetype for: " + fileStatus.getPath());
			}
		}
	}
}