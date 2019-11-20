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
		this.hintMap = new HashMap<String, List<Integer>>();
		this.passwordMap = new HashMap<Integer, Password>();
		this.workerInUseMap = new HashMap<ActorRef, Boolean>();
		this.hashMap = new HashMap<String, String>();
		this.mutations = new ArrayList<char[]>();
		this.tasksPipe = new ArrayList<WorkloadMessage>();
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
	public static class CalculateHashesMessage implements WorkloadMessage {
		private String[] hash;
		public String getIdentifier(){ return "CalculateHashes: ".concat(this.hash[0]); };
	}


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CalculateHeapMessage implements WorkloadMessage {
		private char[] heap;
		private int length;
		public String getIdentifier(){ return "CalculateHeap: ".concat(new String(this.heap)); }
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackPasswordMessage implements WorkloadMessage {
		private Password entity;
		private int length;
		private char[] universe;
		public String getIdentifier() { return "CrackPassword: ".concat(String.valueOf(this.entity.getId())); }
	}

	/////////////////
	// Actor State //
	/////////////////

	@Data
	protected class Password {
		private int Id;
		private String name;
		private HashMap<String, String> hints;
		private String encodedPassword;
		private String decodedPassword;


        public Password(int Id, String name, String password, String[] hints){
		    this.Id = Id;
		    this.name = name;
		    this.encodedPassword = password;
		    this.decodedPassword = "";
		    this.hints = new HashMap<>();
		    for(String hint: hints){
		    	this.hints.put(hint, "");
			}
        }

        public boolean addDecodedHint(String hash, String hint){
			this.hints.put(hash, hint);
			return this.hints.values().contains("");
		}
	}

	private final ActorRef reader;
	private final ActorRef collector;

	private long startTime;

	private int pLength;
	private char[] pChars;

	private HashMap<String, String> hashMap;
	// hint hash to an int (id)
	private HashMap<String, List<Integer>> hintMap;
	// id to the password class
	private HashMap<Integer, Password> passwordMap;
	// way to identify if worker is in use or not boolean value inUse = true; notinUse = false
	private HashMap<ActorRef, Boolean> workerInUseMap;

	private List<char[]> mutations;
	private List<WorkloadMessage> tasksPipe;

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
				.match(Worker.HeapCalculatedMessage.class, this::handle) // calculated a heap
                .match(Worker.HashCalculatedMessage.class, this::handle) // When worker sends work calculated
				.match(Worker.PasswordCrackedMessage.class, this::handle) // Password got cracked
				.match(Worker.WorkerFreeMessage.class, this::handle) // When worker is free again
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void getMutations(char[] data, int dlength, int length, char[] imt, int idx, int i, List<char[]> list){
		if(idx == length){
			list.add(imt);
			return;
		}
		if(i >= dlength) {
			return;
		}
		imt[idx] = data[i];
		System.out.println(imt);
		getMutations(data, dlength, length, imt, idx+1, i+1, list);
		getMutations(data, dlength, length, imt, idx, i+1, list);
	}

	private void addTask(WorkloadMessage m){
		this.tasksPipe.add(m);
		if(assignTask()){
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}
	}

	private boolean assignTask(){
		int freeWorkers = 0;
		for(boolean occupied: workerInUseMap.values()){
			if(!occupied){
				freeWorkers++;
			}
		}
		this.log().info("Having {} open tasks for {} workers", this.tasksPipe.size(), freeWorkers);
		while(this.tasksPipe.size() != 0 && this.workerInUseMap.values().contains(false)){
			for (Map.Entry<ActorRef, Boolean> worker: this.workerInUseMap.entrySet()){
				if(!worker.getValue()){
					WorkloadMessage task =this.tasksPipe.remove(0);
					worker.getKey().tell(task, this.self());
					occupyWorker(worker.getKey());
					this.log().info("Now assigning task {}", task.getIdentifier());
					break;
				}
			}
		}
		for(Password pw: this.passwordMap.values()){
			if(pw.getDecodedPassword() == ""){
				return false;
			}
		}
		return this.passwordMap.size() != 0 && tasksPipe.size() == 0;
	}

	private void occupyWorker(ActorRef worker) {
		workerInUseMap.put(worker, true);
	}

	private void  freeWorker(ActorRef worker) {
		workerInUseMap.put(worker, false);
		if(assignTask()){
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}
	}

	private void logSolution(Password pw){
		this.collector.tell(new Collector.CollectMessage(
						"Cracked Password of "
								.concat(pw.getName()).
								concat(" with ID ")
								.concat(String.valueOf(pw.getId()))
								.concat(". The password is ")
								.concat(pw.getDecodedPassword())),
				this.self());
	}

	private void crackPassword(Password pw, String encoded, String decoded){
		if(pw.addDecodedHint(encoded, decoded)){
			addTask(new CrackPasswordMessage(pw, this.pLength, this.pChars));
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
			return;
		}
		// We want to retrieve the password length and password chars from the lines
		// And then add it to the state (and maybe throw an error if the state is'nt the same as read)
		if(this.pLength != Integer.parseInt(message.getLines().get(0)[3])) {
			this.pLength = Integer.parseInt(message.getLines().get(0)[3]);
			this.pChars = message.getLines().get(0)[2].toCharArray();
			List<char[]> mutations = new ArrayList<>();
			getMutations(this.pChars, this.pChars.length, this.pLength, new char[this.pLength], 0, 0, mutations);
			for(char[] mutation: mutations){
				this.log().info(new String(mutation));
				addTask(new CalculateHeapMessage(mutation, this.pLength));
			}
		}

		for (String[] line : message.getLines()) {
			// We also want to start creating this wonderful hashmap where we store the hash as key with the corresponding string it generates
			int Id = Integer.parseInt(line[0]);
			String name = line[1];
			String password = line[4];
			String[] hints = IntStream.range(5, line.length-1).mapToObj(i -> line[i]).toArray(String[]::new);
			Password pw = new Password(Id, name, password, hints);
            this.passwordMap.put(Id, pw);
            for(String hint : hints){
            	List<Integer> l = this.hintMap.getOrDefault(hint, new ArrayList<>());
            	if(l.size() == 0){
					this.hintMap.put(hint, new ArrayList<Integer>(Id));
				} else {
            		String decoded = this.passwordMap.get(l.get(0)).getHints().get(hint);
            		crackPassword(pw, hint, decoded);
            		l.add(Id);
				}
			}
		}

		this.log().info("Finished Setup");
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		// this.reader.tell(new Reader.ReadMessage(), this.self());
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
		freeWorker(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workerInUseMap.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}

	protected void handle(Worker.HeapCalculatedMessage message){
		Iterator<String[]> iter = new Iterator<String[]>() {
			private int idx =0 ;
			@Override
			public boolean hasNext() {
				return idx < message.getHeaps().size()-1;
			}

			@Override
			public String[] next() {
				int odx = idx;
				idx+=1000;
				if(idx > message.getHeaps().size()-1){
					idx = message.getHeaps().size()-1;
				}
				return IntStream.range(odx, idx).mapToObj(c -> message.getHeaps().get(c)).toArray(String[]::new);
			}
		};
		while(iter.hasNext()){
			addTask(new CalculateHashesMessage(iter.next()));
		}
	}

	protected void handle(Worker.HashCalculatedMessage message){
		this.hashMap.put(message.getEncoded(), message.getDecoded());
	    // retrieve the password id or a default
	    List<Integer> idx = this.hintMap.getOrDefault(message.getEncoded(), new ArrayList<>());
	    // is there an encoded hint
	    if(idx.size() == 0){
	    	for(int x :idx) {
				// we get the password class from the passwordmap
				Password pw = this.passwordMap.get(x);
				// adding decoded Hint to array;
				this.log().info("Cracked one hash");
				crackPassword(pw, message.getEncoded(), message.getDecoded());
			}
        }
    }

    protected void handle(Worker.PasswordCrackedMessage message){
		Password pw = this.passwordMap.get(message.getId());
		pw.setDecodedPassword(message.getDecodedPassword());
	}

	protected void handle(Worker.WorkerFreeMessage message){
		freeWorker(this.sender());
	}
}
