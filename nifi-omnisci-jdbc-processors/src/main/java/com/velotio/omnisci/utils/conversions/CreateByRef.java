package com.velotio.omnisci.utils.conversions;

import java.util.concurrent.Callable;
public class CreateByRef {
	private static final ThreadLocal<Boolean> BY_REF_TL = new ThreadLocal<Boolean>() {
	    @Override
	    protected Boolean initialValue() {
	      return Boolean.FALSE;
	    }
	  };

	  private CreateByRef() {}

	  public static boolean isByRef() {
	    return BY_REF_TL.get() == Boolean.TRUE;
	  }

	  public static <T> T call(Callable<T> callable) throws Exception{
	    boolean alreadyByRef = BY_REF_TL.get() == Boolean.TRUE;
	    try {
	      if (!alreadyByRef) {
	        BY_REF_TL.set(Boolean.TRUE);
	      }
	      return callable.call();
	    } finally {
	      if (!alreadyByRef) {
	        BY_REF_TL.set(Boolean.FALSE);
	      }
	    }
	  }
}
