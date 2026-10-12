package edu.usp.cs324.api;

import java.io.Serializable;
import java.util.List;

public record Status(Peer peer, long jac, int assignedJobs, Term term,
                     List<Peer> neighbours) implements Serializable { }
