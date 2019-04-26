package com.velotio.omnisci.utils.conversions;

public class ByteArrayTypeSupport extends TypeSupport<byte[]> {
	@Override
	  public byte[] convert(Object value) {
	    if (value instanceof byte[]) {
	      return (byte[])value;
	    }
	    throw new IllegalArgumentException("");
	  }

	  @Override
	  public Object convert(Object value, TypeSupport targetTypeSupport) {
	    if (targetTypeSupport instanceof ByteArrayTypeSupport) {
	      return value;
	    } else {
	      throw new IllegalArgumentException("");
	    }
	  }

	  @Override
	  public Object create(Object value) {
	    return clone(value);
	  }

	  @Override
	  public Object get(Object value) {
	    return clone(value);
	  }

	  @Override
	  public Object clone(Object value) {
	    return ((byte[])value).clone();
	  }

	  @Override
	  public boolean equals(Object value1, Object value2) {
	    return (value1 == value2) || (value1 != null && value2 != null && arrayEquals((byte[])value1, (byte[])value2));
	  }

	  private static boolean arrayEquals(byte[] arr1, byte[] arr2) {
	    boolean eq = false;
	    if (arr1.length == arr2.length) {
	      eq = true;
	      for (int i = 0; eq && i < arr1.length; i++) {
	        if (arr1[i] != arr2[i]) {
	          eq = false;
	        }
	      }
	    }
	    return eq;
	  }

}
