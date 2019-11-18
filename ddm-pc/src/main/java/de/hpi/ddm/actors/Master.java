package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
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
		this.workers = new ArrayList<>();
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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CalculateHashMessage {
		private char[] chars;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	@Data
	@AllArgsConstructor
	private class Password {
		private int ID;
		private String name;
	}

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;

	private int pLength;
	private char[] pChars;

	private HashMap<String, String> hintMap = new HashMap<String, String>();
	private HashMap<String, Password> passwordMap = new HashMap<String, Password>();

	private List<char[]> mutations = new ArrayList<char[]>();

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
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
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
			this.mutations.add(sub);
		}

		// As soon as we know this we want to start calculating and shooting messages to every available worker
		// We want to cleverly chunk the options here...
		// Maybe we should delegate this even further from the worker
		for(ActorRef worker: this.workers){
			worker.tell(new CalculateHashMessage(mutations.get(mutations.size()-1)), this.self());
			mutations.remove(mutations.size()-1);
		}

		for (String[] line : message.getLines()) {
			// We also want to start creating this wonderful hashmap where we store the hash as key with the corresponding string it generates
			this.passwordMap.put(line[4], new Password(Integer.parseInt(line[0]), line[1]));
			for(String hash : IntStream.range(5, line.length).mapToObj(i -> line[i]).toArray(String[]::new)){
				this.hintMap.put(hash, "");
			}
			System.out.println(Arrays.toString(line));
		}

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
