package com.ddcoding.common.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigHelper {

	@SuppressWarnings("unchecked")
	public static <T> T getOrElse(Config config, String path, T defaultValue) throws Exception {
		T finalValue = null;
		try {
			if (defaultValue == null) {
				throw new IllegalArgumentException("default == null, but T type is undefined for key $path");
			}
			Object value = config.getAnyRef(path);
			finalValue = ((T) value);
		} catch (ConfigException e) {
			finalValue = defaultValue;
		} catch (Exception e) {
			throw e;
		}

		return finalValue;
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> getArrayOrElse(Config config, String path, List<T> defaultValue) throws Exception {
		List<T> finalValue = new ArrayList<T>();
		try {
			List<? extends Object> values = config.getAnyRefList(path);
			for (Object value : values) {
				finalValue.add(((T) value));
			}
		} catch (ConfigException e) {
			finalValue.addAll(defaultValue);
		} catch (Exception e) {
			throw e;
		}

		return finalValue;
	}
	
	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> getMapOrElse(Config config, String path, Map<K, V> defaultValue) throws Exception {
		Map<K, V> finalValue = new HashMap<K, V>();
		try {
			List<String> values = config.getStringList(path);
			for (String value : values) {
				String [] kv = value.split(":");
 				finalValue.put(((K) kv[0]), ((V) kv[1]));
			}
		} catch (ConfigException e) {
			finalValue.putAll(defaultValue);
		} catch (Exception e) {
			throw e;
		}

		return finalValue;
	}
}
