package com.sksamuel.pulsar4s.cats

import cats.effect.Resource.ExitCase
import cats.effect.{Async, IO, Resource}
import cats.implicits._
import com.sksamuel.exts.Logging
import com.sksamuel.pulsar4s
import com.sksamuel.pulsar4s._
import com.sksamuel.pulsar4s.conversions.collections._
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.transaction.Transaction
import org.apache.pulsar.client.api.{
  Consumer => _,
  MessageId => _,
  Producer => _,
  PulsarClient => _,
  Reader => _,
  _
}

import java.util.concurrent._
import scala.concurrent.ExecutionException
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CatsAsyncHandler extends CatsAsyncHandlerLowPriority {
  implicit def handler: AsyncHandler[IO] = asyncHandlerForCatsEffectAsync[IO]
}

trait CatsAsyncHandlerLowPriority {

  implicit def asyncHandlerForCatsEffectAsync[F[_]: Async]: AsyncHandler[F] =
    new AsyncHandler[F] with Logging {

      private val F = Async[F]

      override def failed(e: Throwable): F[Nothing] = F.raiseError(e)

      override def createProducer[T](
          builder: ProducerBuilder[T]
      ): F[Producer[T]] = F
        .fromCompletableFuture(F.delay { builder.createAsync() })
        .map(new DefaultProducer(_))

      override def createConsumer[T](
          builder: ConsumerBuilder[T]
      ): F[Consumer[T]] = F
        .fromCompletableFuture(
          F.delay {
            logger.debug("Create consumer async... for builder. ")
            builder.subscribeAsync()
          }
        )
        .map(new DefaultConsumer(_))

      override def createReader[T](
          builder: ReaderBuilder[T]
      ): F[pulsar4s.Reader[T]] = F
        .fromCompletableFuture(F.delay { builder.createAsync() })
        .map(new DefaultReader(_))

      override def send[T](t: T, producer: api.Producer[T]): F[MessageId] =
        F.fromCompletableFuture(F.delay { producer.sendAsync(t) })
          .map(MessageId.fromJava)

      override def receive[T](
          consumer: api.Consumer[T]
      ): F[ConsumerMessage[T]] = F
        .fromCompletableFuture(F.delay { consumer.receiveAsync() })
        .map(ConsumerMessage.fromJava)

      override def receiveBatch[T](
          consumer: api.Consumer[T]
      ): F[Vector[ConsumerMessage[T]]] =
        F.fromCompletableFuture(F.delay { consumer.batchReceiveAsync() })
          .map(_.asScala.map(ConsumerMessage.fromJava).toVector)

      override def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit] =
        F.fromCompletableFuture(F.delay { consumer.unsubscribeAsync() }).void

      override def getLastMessageId[T](
          consumer: api.Consumer[T]
      ): F[MessageId] =
        F.fromCompletableFuture(F.delay { consumer.getLastMessageIdAsync })
          .map(MessageId.fromJava)

      override def close(producer: api.Producer[_]): F[Unit] =
        F.fromCompletableFuture(F.delay { producer.closeAsync() }).void

      override def close(consumer: api.Consumer[_]): F[Unit] =
        F.fromCompletableFuture(F.delay { consumer.closeAsync() }).void

      override def seekAsync(
          consumer: api.Consumer[_],
          messageId: MessageId
      ): F[Unit] =
        F.fromCompletableFuture(F.delay { consumer.seekAsync(messageId) }).void

      override def seekAsync(
          reader: api.Reader[_],
          messageId: MessageId
      ): F[Unit] =
        F.fromCompletableFuture(F.delay { reader.seekAsync(messageId) }).void

      override def seekAsync(reader: api.Reader[_], timestamp: Long): F[Unit] =
        F.fromCompletableFuture(F.delay { reader.seekAsync(timestamp) }).void

      override def transform[A, B](t: F[A])(fn: A => Try[B]): F[B] =
        t.flatMap { a =>
          fn(a) match {
            case Success(b) => F.pure(b)
            case Failure(e) => F.raiseError(e)
          }
        }

      override def acknowledgeAsync[T](
          consumer: api.Consumer[T],
          messageId: MessageId
      ): F[Unit] =
        F.fromCompletableFuture(
          F.delay(consumer.acknowledgeAsync(messageId))
        ).void

      override def acknowledgeAsync[T](
          consumer: api.Consumer[T],
          messageId: MessageId,
          txn: Transaction
      ): F[Unit] =
        F.fromCompletableFuture(
          F.delay(consumer.acknowledgeAsync(messageId, txn))
        ).void

      override def acknowledgeCumulativeAsync[T](
          consumer: api.Consumer[T],
          messageId: MessageId
      ): F[Unit] =
        F.fromCompletableFuture(
          F
            .delay(consumer.acknowledgeCumulativeAsync(messageId))
        ).void

      override def acknowledgeCumulativeAsync[T](
          consumer: api.Consumer[T],
          messageId: MessageId,
          txn: Transaction
      ): F[Unit] =
        F.fromCompletableFuture(
          F.delay(consumer.acknowledgeCumulativeAsync(messageId, txn))
        ).void

      override def negativeAcknowledgeAsync[T](
          consumer: api.Consumer[T],
          messageId: MessageId
      ): F[Unit] = F.delay { consumer.negativeAcknowledge(messageId) }

      override def close(reader: api.Reader[_]): F[Unit] =
        F.fromCompletableFuture(F.delay { reader.closeAsync() }).void

      override def flush(producer: api.Producer[_]): F[Unit] =
        F.fromCompletableFuture(F.delay { producer.flushAsync() }).void

      override def close(client: api.PulsarClient): F[Unit] =
        F.fromCompletableFuture(F.delay { client.closeAsync() }).void

      override def nextAsync[T](reader: api.Reader[T]): F[ConsumerMessage[T]] =
        F.fromCompletableFuture(
          F.delay { reader.readNextAsync() }
        ).map(ConsumerMessage.fromJava)

      override def hasMessageAvailable(reader: api.Reader[_]): F[Boolean] =
        F.fromCompletableFuture(
          F.delay { reader.hasMessageAvailableAsync }
        ).map(identity(_))

      override def send[T](builder: TypedMessageBuilder[T]): F[MessageId] =
        F.fromCompletableFuture(F.delay(builder.sendAsync()))
          .map(MessageId.fromJava)

      override def withTransaction[E, A](
          builder: api.transaction.TransactionBuilder,
          action: TransactionContext => F[Either[E, A]]
      ): F[Either[E, A]] = {
        Resource
          .makeCase(startTransaction(builder)) { (txn, exitCase) =>
            if (exitCase == ExitCase.Succeeded) F.unit else txn.abort
          }
          .use { txn =>
            action(txn).flatMap { result =>
              (if (result.isRight) txn.commit else txn.abort).map(_ => result)
            }
          }
      }

      def startTransaction(
          builder: api.transaction.TransactionBuilder
      ): F[TransactionContext] =
        F.fromCompletableFuture(F.delay(builder.build()))
          .map(TransactionContext(_))

      def commitTransaction(txn: Transaction): F[Unit] =
        F.fromCompletableFuture(F.delay(txn.commit())).map(_ => ())

      def abortTransaction(txn: Transaction): F[Unit] =
        F.fromCompletableFuture(F.delay(txn.abort())).map(_ => ())

    }

}
