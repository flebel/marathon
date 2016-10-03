package mesosphere.marathon.storage

import mesosphere.marathon.ZookeeperConf
import mesosphere.marathon.core.storage.store.impl.cache.LazyCachingPersistenceStore

trait StorageConf extends ZookeeperConf {
  lazy val internalStoreBackend = opt[String](
    "internal_store_backend",
    descr = s"The backend storage system to use. One of ${TwitterZk.StoreName}, ${MesosZk.StoreName}, ${InMem.StoreName}, ${CuratorZk.StoreName}",
    hidden = true,
    validate = Set(TwitterZk.StoreName, MesosZk.StoreName, InMem.StoreName, CuratorZk.StoreName).contains,
    default = Some(CuratorZk.StoreName)
  )

  lazy val storeCache = toggle(
    "store_cache",
    default = Some(true),
    noshort = true,
    descrYes = "(Default) Enable an in-memory cache for the storage layer.",
    descrNo = "Disable the in-memory cache for the storage layer. ",
    prefix = "disable_"
  )

  lazy val maxVersions = opt[Int](
    "zk_max_versions", // while called Zk, applies to every store but the name is kept
    descr = "Limit the number of versions, stored for one entity.",
    default = Some(50)
  )

  lazy val zkMaxConcurrency = opt[Int](
    "zk_max_concurrency",
    default = Some(32),
    hidden = true,
    descr = "Max outstanding requests to Zookeeper persistence"
  )

  lazy val cacheMaxVersionedValueCacheSize = opt[Int](
    "cache_max_versioned_value_cache_size",
    default = Some(LazyCachingPersistenceStore.VersionedValueCacheConfig.Default.maxEntries),
    hidden = true,
    descr = "Max number of versioned objects that are lazy cached per-repository"
  )

  lazy val cachePurgeCountVersionedValuesPerCycle = opt[Int](
    "cache_purge_count_versioned_values_per_cycle",
    default = Some(LazyCachingPersistenceStore.VersionedValueCacheConfig.Default.purgeCount),
    hidden = true,
    descr = "Upon exceeding cache_max_versioned_value_cache_size, purge this number of objects from the lazy cache"
  )

  lazy val cachePRemoveFromVersionedValuesCache = opt[Double](
    "cache_premove_versioned_value",
    default = Some(LazyCachingPersistenceStore.VersionedValueCacheConfig.Default.pRemove),
    hidden = true,
    descr = "Probability that, during a lazy versioned value cache purge, that an entry will be removed"
  )
}
