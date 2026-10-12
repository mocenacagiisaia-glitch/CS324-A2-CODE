package edu.usp.cs324.api;

import java.rmi.RemoteException;

/** Pre-admission rejection only: retrying cannot duplicate an accepted job. */
public class RetryException extends RemoteException {
    public RetryException(String message) { super(message); }
}
