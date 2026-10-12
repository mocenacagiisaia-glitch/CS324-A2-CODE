package edu.usp.cs324.api;

import java.io.Serializable;

public record Candidate(Peer peer, long jac) implements Serializable { }
