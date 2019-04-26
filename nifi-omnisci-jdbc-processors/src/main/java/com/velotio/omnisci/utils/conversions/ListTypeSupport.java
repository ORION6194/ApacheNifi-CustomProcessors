package com.velotio.omnisci.utils.conversions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
public class ListTypeSupport extends TypeSupport<List> {
	@Override
	  public List convert(Object value) {
	    if (value instanceof List) {
	      return (List) value;
	    }
	    throw new IllegalArgumentException("");
	  }

	  @Override
	  public Object convert(Object value, TypeSupport targetTypeSupport) {
	    if (targetTypeSupport instanceof ListTypeSupport) {
	      return value;
	    } else if(targetTypeSupport instanceof ListMapTypeSupport) {
	      List list = (List) value;
	      LinkedHashMap<String, Field> listMap = new LinkedHashMap<>(list.size());
	      for (int i = 0; i < list.size(); i++) {
	        listMap.put(Integer.toString(i), (Field)list.get(i));
	      }
	      return listMap;
	    } else {
	      throw new IllegalArgumentException("");
	    }
	  }

	  @Override
	  @SuppressWarnings("unchecked")
	  public Object clone(Object value) {
	    List list = null;
	    if (value != null) {
	      list = deepCopy((List<Field>)value);
	    }
	    return list;
	  }

	  private static List<Field> deepCopy(List<Field> list) {
	    List<Field> copy = new ArrayList<>(list.size());
	    for (int i = 0; i < list.size(); i++) {
	      Field field = list.get(i);
	      Utils.checkNotNull(field, Utils.formatL("List has null element at '{}' pos", i));
	      copy.add(field.clone());
	    }
	    return copy;
	  }

	  @Override
	  public Object create(Object value) {
	    return clone(value);
	  }
}
