/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package com.cealiy.hdfs;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@Private
@SuppressWarnings({"unused","unchecked","rawtypes"})
public class HttpFSUtils {
	public static final String SERVICE_NAME = "/webhdfs";
	public static final String SERVICE_VERSION = "/v1";
	
	private static final String SERVICE_PATH = "/webhdfs/v1";

	static URL createURL(Path path, Map<String, String> params) throws IOException {
		return createURL(path, params, (Map) null);
	}

	static URL createURL(Path path, Map<String, String> params, Map<String, List<String>> multiValuedParams)
			throws IOException {
		URI uri = path.toUri();
		String realScheme;
		if (uri.getScheme().equalsIgnoreCase("webhdfs")) {
			realScheme = "http";
		} else {
			if (!uri.getScheme().equalsIgnoreCase("swebhdfs")) {
				throw new IllegalArgumentException(MessageFormat
						.format("Invalid scheme [{0}] it should be \'webhdfs\' or \'swebhdfs\'", new Object[] { uri }));
			}

			realScheme = "https";
		}

		StringBuilder sb = new StringBuilder();
		sb.append(realScheme).append("://").append(uri.getAuthority()).append("/webhdfs/v1").append(uri.getPath());
		String separator = "?";

		Iterator arg6;
		Entry multiValuedEntry;
		for (arg6 = params.entrySet().iterator(); arg6.hasNext(); separator = "&") {
			multiValuedEntry = (Entry) arg6.next();
			sb.append(separator).append((String) multiValuedEntry.getKey()).append("=")
					.append(URLEncoder.encode((String) multiValuedEntry.getValue(), "UTF8"));
		}

		if (multiValuedParams != null) {
			arg6 = multiValuedParams.entrySet().iterator();

			while (arg6.hasNext()) {
				multiValuedEntry = (Entry) arg6.next();
				String name = URLEncoder.encode((String) multiValuedEntry.getKey(), "UTF8");
				List values = (List) multiValuedEntry.getValue();

				for (Iterator arg10 = values.iterator(); arg10.hasNext(); separator = "&") {
					String value = (String) arg10.next();
					sb.append(separator).append(name).append("=").append(URLEncoder.encode(value, "UTF8"));
				}
			}
		}

		return new URL(sb.toString());
	}

	static Object jsonParse(HttpURLConnection conn) throws IOException {
		try {
			JSONParser ex = new JSONParser();
			return ex.parse(new InputStreamReader(conn.getInputStream(), Charsets.UTF_8));
		} catch (ParseException arg1) {
			throw new IOException("JSON parser error, " + arg1.getMessage(), arg1);
		}
	}
}