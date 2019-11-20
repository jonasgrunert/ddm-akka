package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	public static class WorkerFreeMessage {}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class CrackedHintMessage {
	    private int Id;
	    private String hash;
	    private String decoded;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class CrackedPasswordMessage {
        private int Id;
        private String cracked;
    }

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	private String hash;
	private String hint;
	private boolean isCracked;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
                .match(Master.CrackHintMessage.class, this::handle)
                .match(Master.CrackPasswordMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(Master.CrackHintMessage message){
	    this.hash = message.getHash();
	    this.isCracked = false;
	    this.hint= "";
	    heapPermutation(message.getUniverse(), message.getUniverse().length);
	    if(this.isCracked)  {
	        this.sender().tell(new CrackedHintMessage(message.getId(), message.getHash(), this.hint), this.self());
        }
	    this.sender().tell(new WorkerFreeMessage(), this.self());
    }

    private void handle(Master.CrackPasswordMessage message){
	    this.hash = message.getPw().getEncodedPassword();
	    this.isCracked = false;
	    this.hint ="";
	    List<Character> alphabet = new ArrayList<Character>();
	    for(char c: message.getUniverse()){
	    	alphabet.add(c);
        }
		for(String hint: message.getPw().getHints().values()){
            int i = 0;
		    while(i < alphabet.size()){
				if(!hint.contains(String.valueOf(alphabet.get(i)))) {
				    alphabet.remove(i);
                } else {
				    i++;
                }
			}
		}
	    char[] abc = alphabet.stream().map(String::valueOf).collect(Collectors.joining()).toCharArray();
		System.out.println(alphabet);
	    generateCombinations(abc, "", message.getLength(), abc.length);
	    if(this.isCracked){
	        this.sender().tell(new CrackedPasswordMessage(message.getPw().getId(), this.hint), this.self());
        } else {
	    	this.sender().tell("Couldn't crack password", this.self());
		}
	    this.sender().tell(new WorkerFreeMessage(), this.self());
    }

    private void generateCombinations(char[] set, String prefix, int n, int k) {
        if(isCracked) return;
	    if (k == 0) {
            if(Objects.equals(hash(prefix), this.hash)){
                this.isCracked = true;
                this.hint = prefix;
            }
            return;
        }
        for (int i = 0; i < n; ++i) generateCombinations(set, prefix + set[i], n, k - 1);
    }

    private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size) {
		// If size is 1, store the obtained permutation
        if(this.isCracked) return;
		if (size == 1) {
		    String s = hash(new String(a));
			if (Objects.equals(s, this.hash)) {
			    this.isCracked = true;
			    this.hint = new String(a);
            }
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}