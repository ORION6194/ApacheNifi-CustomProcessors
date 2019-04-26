package com.velotio.omnisci.utils.conversions;

public  abstract class TypeSupport<T> {
	public abstract T convert(Object value);

	  public Object convert(Object value, TypeSupport targetTypeSupport) {
	    return targetTypeSupport.convert(value);
	  }

	  public Object create(Object value) {
	    return value;
	  }

	  public Object get(Object value) {
	    return value;
	  }

	  // default implementation assumes value is immutable, no need to clone
	  public Object clone(Object value) {
	    return value;
	  }

	  public boolean equals(Object value1, Object value2) {
	    return (value1 == value2) || (value1 != null && value2 != null && value1.equals(value2));
	  }
}
