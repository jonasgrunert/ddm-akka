package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import com.sun.org.apache.xpath.internal.operations.Bool;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Int;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.hashMap = new HashMap<String, String>();
		this.hintMap = new HashMap<String, Integer>();
		this.passwordMap = new HashMap<Integer, Password>();
		this.workerInUseMap = new HashMap<ActorRef, Boolean>();
		this.taskAssignment = new HashMap<String, ActorRef>();
		this.tasks = new HashMap<String, WorkloadMessage>();
		this.mutations = new ArrayList<char[]>();
		this.heaps = new ArrayList<String>();
		this.tasksPipe = new ArrayList<String>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	public interface WorkloadMessage {
		String getIdentifier();
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CalculateHashMessage implements WorkloadMessage {
		private String hash;
		public String getIdentifier(){
			return "CalculateHash: ".concat(this.hash);
		};
	}


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CalculateHeapMessage implements WorkloadMessage {
		private char[] heap;
		private int length;
		public String getIdentifier(){
			return "CalculateHeap: ".concat(this.heap.toString());
		}
	}

	/////////////////
	// Actor State //
	/////////////////

	@Data
	private class Password {
		private int ID;
		private String name;
		private String[] encodedHints;
		private String[] decodedHints;
		private String encodedPassword;
		private String decodedPassword;


        public Password(int Id, String name, String password, String[] hints){
		    this.ID = Id;
		    this.name = name;
		    this.encodedPassword = password;
		    this.encodedHints = hints;
		    this.decodedHints = new String[hints.length];
        }
	}

	private final ActorRef reader;
	private final ActorRef collector;

	private long startTime;

	private int pLength;
	private char[] pChars;

	// hash to letter combination
	private HashMap<String, String> hashMap;
	// hint hash to an int (id)
	private HashMap<String, Integer> hintMap;
	// id to the password class
	private HashMap<Integer, Password> passwordMap;
	// way to identify if worker is in use or not
	private HashMap<ActorRef, Boolean> workerInUseMap;
	// way to find out which task is assigned to which worker
	private HashMap<String, ActorRef> taskAssignment;
	// list of available tasks
	private HashMap<String,WorkloadMessage> tasks;

	private List<char[]> mutations;
	private List<String> heaps;
	private List<String> tasksPipe;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(BatchMessage.class, this::handle) //1
				.match(Worker.RegisterToWorkloadMessage.class, this::handle)
				.match(Worker.HeapCalculatedMessage.class, this::handle)//2 TODO: Add message reception of worker acknowledgment and task reception
				// 3 TODO: Add handler for worker assignment on parallel list
                .match(Worker.HashCalculatedMessage.class, this::handle) //When worker sends work calculated
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	// TODO Handle Freeing and occupying workers
	protected void assignTask(){
		while(this.tasks.size() != 0 && this.workerInUseMap.values().contains(true)){
			for (Map.Entry<ActorRef, Boolean> worker: this.workerInUseMap.entrySet()){
				if(!worker.getValue()){
					worker.getKey().tell(this.tasks.get(this.tasksPipe.get(0)), this.self());
				}
			}
		}
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		// We want to retrieve the password length and password chars from the lines
		// And then add it to the state (and maybe throw an error if the state is'nt the same as read)
		this.pLength = Integer.parseInt(message.getLines().get(0)[3]);
		this.pChars = message.getLines().get(0)[2].toCharArray();
		// now we can add all permutations with one missing letter to our state
		for(int i = 0; i < this.pChars.length; i++){
			char[] sub = new char[this.pChars.length-1];
			int k = 0;
			for (int j = 0; j < this.pChars.length; j++) {
				if(j!=i) {
					sub[k++] = this.pChars[j];
				}
			}
			System.out.println(sub);
			this.mutations.add(sub);
		}

		// As soon as we know this we want to start calculating and shooting messages to every available worker
		// We want to cleverly chunk the options here...
		// Maybe we should delegate this even further from the worker
		for(char[] mutation: this.mutations){
			WorkloadMessage m = new CalculateHeapMessage(mutation, this.pLength);
			this.tasks.put(m.getIdentifier(), m);
			this.tasksPipe.add(m.getIdentifier());
		}

		assignTask();

		//TODO: Move this into another response (when master is sure that workers got the work load)
		for (String[] line : message.getLines()) {
			// We also want to start creating this wonderful hashmap where we store the hash as key with the corresponding string it generates
			int Id = Integer.parseInt(line[0]);
			String name = line[1];
			String password = line[4];
			String[] hints = IntStream.range(5, line.length).mapToObj(i -> line[i]).toArray(String[]::new);
            this.passwordMap.put(Id, new Password(Id, name, password, hints));
			this.hashMap.put(password, "");
            for(String hint : hints){
				this.hintMap.put(hint, Id);
				this.hashMap.put(hint, "");
			}
			System.out.println(Arrays.toString(line));
		}

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workerInUseMap.keySet()) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workerInUseMap.put(this.sender(), false);
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workerInUseMap.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}

	protected void handle(Worker.RegisterToWorkloadMessage message) {
		workerInUseMap.put(this.sender(), true);
		taskAssignment.put(message.getIdentifier(),this.sender());
		this.tasks.remove(message.getIdentifier());
		this.tasksPipe.remove(message.getIdentifier());
	}

	protected void handle(Worker.HeapCalculatedMessage message){
		for(String heap: message.getHeaps()){
			if(!heaps.contains(heap)){
				heaps.add(heap);
			}
		}
	}

	protected void handle(Worker.HashCalculatedMessage message){
		// put it in the hashmap for later use
	    this.hashMap.put(message.getEncoded(), message.getDecoded());
	    // retrieve the password id or a default
	    int idx = this.hintMap.getOrDefault(message.getEncoded(), -1);
	    // is there an encoded hint
	    if(idx != -1){
	    	// we get the password class from the passwordmap
	        Password pw = this.passwordMap.get(idx);
	        // adding decoded Hint to array;
	        for(int i= 0; i< pw.encodedHints.length; i++){
	            if(pw.encodedHints[i] == message.getEncoded()){
	                pw.decodedHints[i] = message.getDecoded();
                }
            }
        }
    }
}
