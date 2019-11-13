package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.function.Creator;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import de.hpi.ddm.structures.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	private static class StreamCompletedMessage {}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class RequestMessage implements Serializable {
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private  static class ConfigurationMessage implements Serializable {
		private ActorRef sender;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	/////////////////
	// Actor State //
	/////////////////

	private List<Byte> incomingRequest = new ArrayList<Byte>();
	private byte[] outgoingRequest = new byte[0];
	private ActorRef receiver;

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////


	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(ConfigurationMessage.class, this::handle)
				.match(RequestMessage.class, this::handle)
				.match(Byte.class, this::handle)
				.match(StreamCompletedMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		this.receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// serializing with kryo and the architecture given
		this.outgoingRequest = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());
		/* building an akka stream
		 * 1. create Source
		 * 2. create sink
		 * 3. materialize
		 */
		/* Additional work
		 * 1. create a remote sink
		 * 2. connect to it
		 * 3. wrap up the stream and give it out
		 */
		receiverProxy.tell(new RequestMessage(this.self(), this.receiver), this.self());
	}

	private void handle(BytesMessage<byte[]> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		this.log().info("Got data");
		message.getReceiver().tell(KryoPoolSingleton.get().fromBytes(message.getBytes()), message.getSender());
	}

	private void handle(ConfigurationMessage message) {
		Creator creator = new Creator() {
			@Override
			public Object create() throws Exception, Exception {
				return new Iterator<Byte>() {
					private int i = 0;

					@Override
					public boolean hasNext() {
						return outgoingRequest.length > i;
					}

					@Override
					public Byte next() {
						return Byte.valueOf(outgoingRequest[i++]);
					}
				};
			}
		};
		Source
				.fromIterator(creator)
				.to(Sink.actorRef(message.getSender(), new StreamCompletedMessage()))
				.run(ActorMaterializer.create(this.context()));
		message.getSender().tell(new StreamCompletedMessage(), this.self());
	}

	private void handle(RequestMessage message) {
		this.receiver = message.getReceiver();
		message.getSender().tell(new ConfigurationMessage(this.self()), this.self());
	}

	private void handle(Byte message) {
		// Assemble the bytes again
		// for the sake of speed you should not this:  this.log().info("Byte arrived");
		this.incomingRequest.add(message);
	}
	private void handle(StreamCompletedMessage message){
		this.log().info("Request ended");
		byte[] bytes = new byte[this.incomingRequest.size()];
		int i = 0;
		while (i < this.incomingRequest.size()) {
			bytes[i] = this.incomingRequest.get(i);
			i++;
		}
		t
		this.receiver.tell(KryoPoolSingleton.get().fromBytes(bytes), this.self());
	}

}