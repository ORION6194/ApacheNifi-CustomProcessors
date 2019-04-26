package com.velotio.omnisci.utils.conversions;
import java.util.LinkedHashMap;
import java.util.Map;
public class MapTypeSupport extends TypeSupport<Map> {
	@Override
	  public Map convert(Object value) {
	    if (value instanceof Map) {
	      return (Map) value;
	    }
	    throw new IllegalArgumentException("");
	  }

	  @Override
	  public Object convert(Object value, TypeSupport targetTypeSupport) {
	    if (targetTypeSupport instanceof MapTypeSupport || targetTypeSupport instanceof ListMapTypeSupport) {
	      return value;
	    } else {
	      throw new IllegalArgumentException("");
	    }
	  }


	  @Override
	  @SuppressWarnings("unchecked")
	  public Object clone(Object value) {
	    Map map = null;
	    if (value != null) {
	      map = deepCopy((Map<String, Field>)value);
	    }
	    return map;
	  }

	  private static Map<String, Field> deepCopy(Map<String, Field> map) {
	    Map<String, Field> copy = new LinkedHashMap<>();
	    for (Map.Entry<String, Field> entry : map.entrySet()) {
	      String name = entry.getKey();
	      Utils.checkNotNull(name, "Map cannot have null keys");
	      Utils.checkNotNull(entry.getValue(), Utils.formatL("Map cannot have null values, key '{}'", name));
	      copy.put(entry.getKey(), entry.getValue().clone());
	    }
	    return copy;
	  }

	  @Override
	  public Object create(Object value) {
	    return clone(value);
	  }

}
