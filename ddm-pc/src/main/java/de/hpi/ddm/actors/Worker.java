package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
	public static class HashCalculatedMessage {
		private String encoded;
		private String decoded;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HeapCalculatedMessage {
		private List<String> heaps;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordCrackedMessage {
		private int Id;
		private String decodedPassword;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
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
				.match(Master.CalculateHashesMessage.class, this::handle) // calculate a single hash
				.match(Master.CalculateHeapMessage.class, this::handle) // calculate heap
				.match(Master.CrackPasswordMessage.class, this::handle) // crack password
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

	private void handle(Master.CalculateHeapMessage message){
		List<String> heaps = new ArrayList<String>();
		heapPermutation(message.getHeap(), message.getLength(), heaps);
		this.sender().tell(new HeapCalculatedMessage(heaps), this.self());
		for(String heap: heaps){
			this.log().info(heap);
		}
		this.sender().tell(new WorkerFreeMessage(), this.self());
	}

	private void handle(Master.CalculateHashesMessage message){
		for(String hint: message.getHash()){
			this.sender().tell(
					new HashCalculatedMessage(hint, hash(hint)),
					this.self()
			);
		}
		this.sender().tell(new WorkerFreeMessage(), this.self());
	}

	private void handle(Master.CrackPasswordMessage message){
		Master.Password pw = message.getEntity();
		List<Character> universe = message.getUniverse().toString().chars().mapToObj(c -> (char) c).collect(Collectors.toList());
		for (String hint: pw.getHints().values()){
			for(char c: message.getUniverse()){
				if(!hint.contains(String.valueOf(c))){
					universe.remove(c);
				}
			}
		}
		List<String> mutations = new ArrayList<String>();
		char[] usedChars = new char[universe.size()];
		for(int i = 0; i< universe.size(); i++){
			usedChars[i] = universe.get(i);
		}
		heapPermutation(usedChars, message.getLength(), mutations);
		for(String mutation: mutations){
			if(hash(mutation) == message.getEntity().getEncodedPassword()){
				this.sender().tell(new PasswordCrackedMessage(message.getEntity().getId(), mutation), this.self());
				return;
			}
		}
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
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

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