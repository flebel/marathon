package mesosphere.marathon.core.storage.store.impl.cache

import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.{ Done, NotUsed }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.Protos.StorageVersion
import mesosphere.marathon.core.storage.store.impl.BasePersistenceStore
import mesosphere.marathon.core.storage.store.{ IdResolver, PersistenceStore }
import mesosphere.util.LockManager

import scala.async.Async.{ async, await }
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

/**
  * A Write Ahead Cache of another persistence store that lazily loads values into the cache.
  *
  * @param store The store to cache
  * @param mat a materializer for Akka Streaming
  * @param ctx The execution context for future chaining.
  * @tparam K The persistence store's primary key type
  * @tparam Serialized The serialized format for the persistence store.
  */
class LazyCachingPersistenceStore[K, Category, Serialized](
    val store: BasePersistenceStore[K, Category, Serialized],
    val versionedValueCacheConfig: LazyCachingPersistenceStore.VersionedValueCacheConfig)(implicit
  mat: Materializer,
    ctx: ExecutionContext) extends PersistenceStore[K, Category, Serialized] with StrictLogging {

  private val lockManager = LockManager.create()
  private[store] val idCache = TrieMap.empty[Category, Seq[Any]]
  private[store] val valueCache = TrieMap.empty[K, Option[Any]]
  private[store] val versionCache = TrieMap.empty[(Category, K), Seq[Any]]
  private[store] val versionedValueCache = TrieMap.empty[(K, OffsetDateTime), Option[Any]]

  override def storageVersion(): Future[Option[StorageVersion]] = store.storageVersion()

  override def setStorageVersion(storageVersion: StorageVersion): Future[Done] =
    store.setStorageVersion(storageVersion)

  @SuppressWarnings(Array("all")) // async/await
  override def ids[Id, V]()(implicit ir: IdResolver[Id, V, Category, K]): Source[Id, NotUsed] = {
    val category = ir.category
    val idsFuture = lockManager.executeSequentially(category.toString) {
      if (idCache.contains(category)) {
        Future.successful(idCache(category).asInstanceOf[Seq[Id]])
      } else {
        async { // linter:ignore UnnecessaryElseBranch
          val children = await(store.ids.toMat(Sink.seq)(Keep.right).run())
          idCache(category) = children
          children
        }
      }
    }
    Source.fromFuture(idsFuture).mapConcat(identity)
  }

  @SuppressWarnings(Array("all")) // async/await
  private def deleteCurrentOrAll[Id, V](
    k: Id,
    delete: () => Future[Done])(implicit ir: IdResolver[Id, V, Category, K]): Future[Done] = {
    val category = ir.category
    val storageId = ir.toStorageId(k, None)
    lockManager.executeSequentially(category.toString) {
      lockManager.executeSequentially(storageId.toString) {
        async { // linter:ignore UnnecessaryElseBranch
          await(delete())
          valueCache.remove(storageId)
          versionedValueCache.retain { case ((k, version), v) => k != storageId }
          val old = idCache.getOrElse(category, Nil) // linter:ignore UndesirableTypeInference
          val children = old.filter(_ != k) // linter:ignore UndesirableTypeInference
          if (children.nonEmpty) { // linter:ignore UnnecessaryElseBranch+UseIfExpression
            idCache.put(category, children)
          } else {
            idCache.remove(category)
          }
          versionCache.remove((category, storageId))
          Done
        }
      }
    }
  }

  override def deleteCurrent[Id, V](k: Id)(implicit ir: IdResolver[Id, V, Category, K]): Future[Done] = {
    deleteCurrentOrAll(k, () => store.deleteCurrent(k))
  }

  override def deleteAll[Id, V](k: Id)(implicit ir: IdResolver[Id, V, Category, K]): Future[Done] = {
    deleteCurrentOrAll(k, () => store.deleteAll(k))
  }

  @SuppressWarnings(Array("all")) // async/await
  override def get[Id, V](id: Id)(implicit
    ir: IdResolver[Id, V, Category, K],
    um: Unmarshaller[Serialized, V]): Future[Option[V]] = {
    val storageId = ir.toStorageId(id, None)
    lockManager.executeSequentially(storageId.toString) {
      val cached = valueCache.get(storageId) // linter:ignore OptionOfOption
      cached match {
        case Some(v: Option[V] @unchecked) =>
          Future.successful(v)
        case _ =>
          async { // linter:ignore UnnecessaryElseBranch
            val value: Option[V] = await(store.get(id))
            valueCache.put(storageId, value)
            value.foreach { v =>
              if (ir.hasVersions) {
                versionedValueCache.put((storageId, ir.version(v)), value)
              }
            }
            value
          }
      }
    }
  }

  override def get[Id, V](id: Id, version: OffsetDateTime)(implicit
    ir: IdResolver[Id, V, Category, K],
    um: Unmarshaller[Serialized, V]): Future[Option[V]] = {
    val storageId = ir.toStorageId(id, None)
    lockManager.executeSequentially(storageId.toString) {
      val cached = versionedValueCache.get((storageId, version)) // linter:ignore OptionOfOption
      cached match {
        case Some(v: Option[V] @unchecked) =>
          Future.successful(v)
        case _ =>
          async { // linter:ignore UnnecessaryElseBranch
            val value = await(store.get(id, version))
            versionedValueCache.put((storageId, version), value)
            value
          }
      }
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  override def store[Id, V](id: Id, v: V)(implicit
    ir: IdResolver[Id, V, Category, K],
    m: Marshaller[V, Serialized]): Future[Done] = {
    val category = ir.category
    val storageId = ir.toStorageId(id, None)
    lockManager.executeSequentially(category.toString) {
      lockManager.executeSequentially(storageId.toString) {
        async { // linter:ignore UnnecessaryElseBranch
          await(store.store(id, v))
          valueCache.put(storageId, Some(v))
          val cachedIds = idCache.getOrElse(category, Nil) // linter:ignore UndesirableTypeInference
          idCache.put(category, id +: cachedIds)
          if (ir.hasVersions) {
            val version = ir.version(v)
            updateCachedVersions(storageId, version, category, v)
          }
          Done
        }
      }
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  override def store[Id, V](id: Id, v: V, version: OffsetDateTime)(implicit
    ir: IdResolver[Id, V, Category, K],
    m: Marshaller[V, Serialized]): Future[Done] = {
    val category = ir.category
    val storageId = ir.toStorageId(id, None)
    lockManager.executeSequentially(category.toString) {
      lockManager.executeSequentially(storageId.toString) {
        async { // linter:ignore UnnecessaryElseBranch
          await(store.store(id, v, version))
          updateCachedVersions(storageId, version, category, v)
          Done
        }
      }
    }
  }

  protected[cache] def maybePurgeCachedVersions(
    toRemove: Int = versionedValueCacheConfig.purgeCount,
    pRemoval: Double = versionedValueCacheConfig.pRemove)(f: () => Boolean): Unit =
    while (f()) {
      // randomly GC the versions
      val counter = new AtomicInteger
      versionedValueCache.retain { (k, v) =>
        val x = scala.util.Random.nextDouble()
        x > pRemoval || counter.incrementAndGet() > toRemove
      }
    }

  protected def updateCachedVersions[V](
    storageId: K,
    version: OffsetDateTime,
    category: Category,
    v: V): Unit = {

    maybePurgeCachedVersions() { () => versionedValueCache.size > versionedValueCacheConfig.maxEntries }
    versionedValueCache.put((storageId, version), Some(v))
    val cached = versionCache.getOrElse((category, storageId), Nil) // linter:ignore UndesirableTypeInference
    versionCache.put((category, storageId), (version +: cached).distinct)
  }

  override def versions[Id, V](id: Id)(implicit ir: IdResolver[Id, V, Category, K]): Source[OffsetDateTime, NotUsed] = {
    val category = ir.category
    val storageId = ir.toStorageId(id, None)
    val versionsFuture = lockManager.executeSequentially(category.toString) {
      lockManager.executeSequentially(storageId.toString) {
        if (versionCache.contains((category, storageId))) {
          Future.successful(versionCache((category, storageId)).asInstanceOf[Seq[OffsetDateTime]])
        } else {
          async { // linter:ignore UnnecessaryElseBranch
            val children = await(store.versions(id).toMat(Sink.seq)(Keep.right).run())
            versionCache((category, storageId)) = children
            children
          }
        }
      }
    }
    Source.fromFuture(versionsFuture).mapConcat(identity)
  }

  override def deleteVersion[Id, V](
    k: Id,
    version: OffsetDateTime)(implicit ir: IdResolver[Id, V, Category, K]): Future[Done] =
    deleteCurrentOrAll(k, () => store.deleteVersion(k, version))

  override def toString: String = s"LazyCachingPersistenceStore($store)"
}

object LazyCachingPersistenceStore {

  def apply[K, Category, Serialized](
    store: BasePersistenceStore[K, Category, Serialized])(implicit
    mat: Materializer,
    ctx: ExecutionContext) = new LazyCachingPersistenceStore(store, VersionedValueCacheConfig.Default)

  case class VersionedValueCacheConfig(
    val maxEntries: Int = MAX_VERSIONED_VALUE_CACHE_SIZE,
    val purgeCount: Int = PURGE_COUNT_FROM_VERSIONED_VALUE_CACHE,
    val pRemove: Double = P_REMOVE_FROM_VERSIONED_VALUE_CACHE
  )

  object VersionedValueCacheConfig {
    val Default = VersionedValueCacheConfig()
  }

  /**
    * max number of entries allowed in versionedValueCache before entries are purged
    */
  val MAX_VERSIONED_VALUE_CACHE_SIZE = 10000
  /**
    * max number of entries to remove during cycle of purging versionedValueCache
    */
  val PURGE_COUNT_FROM_VERSIONED_VALUE_CACHE = 500
  /**
    * probability that, during a purge of versionedValueCache, a given entry will be removed
    */
  val P_REMOVE_FROM_VERSIONED_VALUE_CACHE = 0.05
}
