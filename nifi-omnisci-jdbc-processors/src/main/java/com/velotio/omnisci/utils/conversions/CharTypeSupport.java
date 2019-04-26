package com.velotio.omnisci.utils.conversions;

public class CharTypeSupport extends TypeSupport<Character> {
	@Override
	  public Character convert(Object value) {
	    if (value instanceof Character) {
	      return (Character) value;
	    }
	    if (value instanceof String) {
	      String s = (String) value;
	      if (s.length() > 0) {
	        return s.charAt(0);
	      }
	    }
	    throw new IllegalArgumentException("");
	  }

}
