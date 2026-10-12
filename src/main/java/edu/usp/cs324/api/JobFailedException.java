package edu.usp.cs324.api;

import java.rmi.RemoteException;

/** A failed job whose started work is known to have finished; never a retry signal. */
public final class JobFailedException extends RemoteException {
    public JobFailedException(String message, Throwable cause) { super(message, cause); }
}
