package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.function.Creator;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.twitter.chill.KryoPool;
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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// serializing with kryo and the architecture given
		byte[] seq = KryoPoolSingleton.get().toBytesWithClass(message.getMessage());
		/* building an akka stream
		 * 1. create Source
		 * 2. create sink
		 * 3. materialize
		 */
		receiverProxy.tell(new BytesMessage<>(seq, this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}