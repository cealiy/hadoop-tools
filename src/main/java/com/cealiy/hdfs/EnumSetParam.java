/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package com.cealiy.hdfs;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.StringUtils;

@Private
@SuppressWarnings({"rawtypes","unchecked"})
public abstract class EnumSetParam<E extends Enum<E>> extends Param<EnumSet<E>> {
	Class<E> klass;

	public EnumSetParam(String name, Class<E> e, EnumSet<E> defaultValue) {
		super(name, defaultValue);
		this.klass = e;
	}

	protected EnumSet<E> parse(String str) throws Exception {
		EnumSet set = EnumSet.noneOf(this.klass);
		if (!str.isEmpty()) {
			String[] arg2 = str.split(",");
			int arg3 = arg2.length;

			for (int arg4 = 0; arg4 < arg3; ++arg4) {
				String sub = arg2[arg4];
				set.add(Enum.valueOf(this.klass, StringUtils.toUpperCase(sub.trim())));
			}
		}

		return set;
	}

	protected String getDomain() {
		return Arrays.asList(this.klass.getEnumConstants()).toString();
	}

	public static <E extends Enum<E>> String toString(EnumSet<E> set) {
		if (set != null && !set.isEmpty()) {
			StringBuilder b = new StringBuilder();
			Iterator i = set.iterator();
			b.append(i.next());

			while (i.hasNext()) {
				b.append(',').append(i.next());
			}

			return b.toString();
		} else {
			return "";
		}
	}

	public String toString() {
		return this.getName() + "=" + toString((EnumSet) this.value);
	}
}