/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package com.cealiy.hdfs;

import java.text.MessageFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public abstract class Param<T> {
	private String name;
	protected T value;

	public Param(String name, T defaultValue) {
		this.name = name;
		this.value = defaultValue;
	}

	public String getName() {
		return this.name;
	}

	public T parseParam(String str) {
		try {
			this.value = str != null && str.trim().length() > 0 ? this.parse(str) : this.value;
		} catch (Exception arg2) {
			throw new IllegalArgumentException(
					MessageFormat.format("Parameter [{0}], invalid value [{1}], value must be [{2}]",
							new Object[] { this.name, str, this.getDomain() }));
		}

		return this.value;
	}

	public T value() {
		return this.value;
	}

	protected abstract String getDomain();

	protected abstract T parse(String arg0) throws Exception;

	public String toString() {
		return this.value != null ? this.value.toString() : "NULL";
	}
}