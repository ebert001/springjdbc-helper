package com.aswishes.spring.exception;

public class RDbException extends RuntimeException {
	private static final long serialVersionUID = -4134923155750269283L;

	public RDbException() {
	}
	
	public RDbException(Throwable cause) {
		super(cause);
	}
	
	public RDbException(String msg) {
		super(msg);
	}
	
	public RDbException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
