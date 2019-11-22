package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
//import com.sun.org.apache.xpath.internal.operations.Bool;
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
		this.passwordMap = new HashMap<Integer, Password>();
		this.workerInUseMap = new HashMap<ActorRef, Boolean>();
		this.universeMessageMapper = new HashMap<>();
		this.universeWorkerMapper = new HashMap<>();
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
	public static class CrackHintMessage implements WorkloadMessage, Serializable {
		private int Id;
		private String hash;
		public String getIdentifier() { return "CrackHint: ".concat(hash); }
	}


	@Data @NoArgsConstructor @AllArgsConstructor
	public static class StartCrackingMessage implements WorkloadMessage, Serializable {
		private char[] universe;
		public String getIdentifier() { return "StartCracking: ".concat(new String(universe)); }
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackPasswordMessage implements WorkloadMessage, Serializable {
		private Password pw;
		private char[] universe;
		private int length;

		@Override
		public String getIdentifier() { return "CrackPassword: ".concat(pw.getName()); }
	}

	/////////////////
	// Actor State //
	/////////////////

	private class NoWorkerFoundException extends Exception{}

	@Data
	protected class Password implements Cloneable, Serializable{
		private int Id;
		private String name;
		private HashMap<String, String> hints;
		private String encodedPassword;
		private String decodedPassword;

		public Object clone(){
			try {
				return super.clone();
			} catch (CloneNotSupportedException e){
				return this;
			}
		}

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
			return !this.hints.values().contains("");
		}
	}

	private final ActorRef reader;
	private final ActorRef collector;

	private long startTime;

	private int pLength;
	private char[] pChars;

	private HashMap<Integer, List<WorkloadMessage>> universeMessageMapper;
	private HashMap<ActorRef, Integer> universeWorkerMapper;
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
				.match(BatchMessage.class, this::handle)
				.match(Worker.CrackedHintMessage.class, this::handle)
				.match(Worker.WorkerFreeMessage.class, this::handle)
				.match(Worker.CrackedPasswordMessage.class, this::handle)
				.match(Worker.WorkerFullMessage.class, this::handle)
				.match(Worker.FreeUniverseMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void getMutations(char[] data, int length, List<char[]> list) {
		char[] imt = new char[data.length - 1];
		for (int x = 0; x < data.length; x++) {
			int i = 0;
			for (int j = 0; j < data.length; j++) {
				if (j != x) {
					imt[i++] = data[j];
				}
			}
			if (imt.length == length) {
				list.add(imt.clone());
			} else {
				getMutations(imt, length, list);
			}
		}
	}

	private void addTask(CrackHintMessage crackHintMessage, int mutation) {
		try {
			List<WorkloadMessage> messagequeue = this.universeMessageMapper.get(mutation);
			messagequeue.add(crackHintMessage);
		} catch (NullPointerException e){
			List<WorkloadMessage> l = new ArrayList<WorkloadMessage>();
			l.add(crackHintMessage);
			this.universeMessageMapper.put(mutation, l);
		}
	}

	private void addTask(StartCrackingMessage m, int u){
		this.universeMessageMapper.get(u).add(m);
	}

	private void addTask(CrackPasswordMessage m){
		this.tasksPipe.add(m);
	}

	private ActorRef getWorkerforUniverse(int i) throws NoWorkerFoundException{
		for(Map.Entry<ActorRef, Integer> e: this.universeWorkerMapper.entrySet()){
			if(e.getValue() == i) return e.getKey();
		}
		throw new NoWorkerFoundException();
	}


	private ActorRef assignTask(WorkloadMessage task) throws NoWorkerFoundException{
		for (Map.Entry<ActorRef, Boolean> worker: this.workerInUseMap.entrySet()){
			if(!worker.getValue()){
				assignTask(task, worker.getKey());
				return worker.getKey();
			}
		}
		throw new NoWorkerFoundException();
	}

	private void assignTask(WorkloadMessage task, ActorRef worker){
		worker.tell(task, this.self());
		occupyWorker(worker);
	}

	private void assignTask(){
		if(this.workerInUseMap.values().contains(false)){
			if(this.tasksPipe.size() != 0){
				WorkloadMessage task = this.tasksPipe.remove(0);
				try { assignTask(task); } catch (NoWorkerFoundException err){};
			}
			for(Map.Entry<Integer, List<WorkloadMessage>> e: this.universeMessageMapper.entrySet()){
				if(e.getValue().size() > 0){
					WorkloadMessage m = e.getValue().get(0);
					try{
						ActorRef worker = getWorkerforUniverse(e.getKey());
						if(!this.workerInUseMap.get(worker)){
							assignTask(m, worker);
							e.getValue().remove(m);
						}
					} catch (NoWorkerFoundException err){
						try{
							ActorRef w = assignTask(m);
							this.universeWorkerMapper.put(w, e.getKey());
							e.getValue().remove(m);
						} catch(NoWorkerFoundException er) {}
					}
				}
			}
		}
	}

	private void occupyWorker(ActorRef worker) {
		workerInUseMap.put(worker, true);
	}

	private void freeWorker(ActorRef worker) {
		workerInUseMap.put(worker, false);
		assignTask();
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
			this.terminate();
			return;
		}
		// We want to retrieve the password length and password chars from the lines
		// And then add it to the state (and maybe throw an error if the state is'nt the same as read)
		if(this.pLength != Integer.parseInt(message.getLines().get(0)[3])) {
			this.pLength = Integer.parseInt(message.getLines().get(0)[3]);
			this.pChars = message.getLines().get(0)[2].toCharArray();
			getMutations(this.pChars, this.pLength, mutations);
		}

		for (String[] line : message.getLines()) {
			// We also want to start creating this wonderful hashmap where we store the hash as key with the corresponding string it generates
			int Id = Integer.parseInt(line[0]);
			String name = line[1];
			String password = line[4];
			String[] hints = IntStream.range(5, line.length).mapToObj(i -> line[i]).toArray(String[]::new);
			Password pw = new Password(Id, name, password, hints);
            this.passwordMap.put(Id, pw);
			for(int i =0; i<  mutations.size(); i++){
				for(String hint : hints){
					addTask(new CrackHintMessage(Id, hint), i);
				}
			}
		}
		for(int i=0; i< mutations.size(); i++){
			addTask(new StartCrackingMessage(mutations.get(i).clone()), i);
		}
		log().info("Finished setup");

		assignTask();

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
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

	protected void handle(Worker.CrackedHintMessage message){

		if(this.passwordMap.get(message.getId()).addDecodedHint(message.getHash(), message.getDecoded())){
			addTask(new CrackPasswordMessage((Password) this.passwordMap.get(message.getId()).clone(), this.pChars.clone(), this.pLength));
		};
	}

	protected void handle(Worker.CrackedPasswordMessage message){
		this.passwordMap.get(message.getId()).setDecodedPassword(message.getCracked());
		logSolution(this.passwordMap.get(message.getId()));
		boolean requestNewBatch = true;
		for(Password pw: this.passwordMap.values()){
			if(pw.getDecodedPassword() == ""){
				requestNewBatch = false;
			}
		}
		if(requestNewBatch){
			System.out.println("ENTRO");
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.reader.tell(new Reader.ReadMessage(), this.self());
		}
	}


	protected void handle(Worker.WorkerFreeMessage message){
		freeWorker(this.sender());
	}

	protected void handle(Worker.WorkerFullMessage message){
		this.sender().tell(new StartCrackingMessage(mutations.get(universeWorkerMapper.get(this.sender())).clone()), this.self());
	}

	protected void handle(Worker.FreeUniverseMessage message) {
		universeWorkerMapper.remove(this.sender());
		assignTask();
	}
}
