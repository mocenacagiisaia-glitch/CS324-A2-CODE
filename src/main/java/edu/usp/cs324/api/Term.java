package edu.usp.cs324.api;

import java.io.Serializable;
import java.util.UUID;

public record Term(long number, UUID electionId, Peer leader) implements Serializable { }
