package kafka.server

import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class ReplicasToPartitions {

  val replicasToPartitionsMap = new mutable.HashMap[Int, mutable.Set[TopicPartition]]()

  def update(
    topicPartition: TopicPartition,
    prevLeader: Option[Int],
    prevReplicas: Set[Int],
    nextLeader: Option[Int],
    nextReplicas: Set[Int],
  ): Unit = {
    if (prevReplicas != nextReplicas) {
      val addedReplicas = nextReplicas.diff(prevReplicas)
      val removedReplica = prevReplicas.diff(nextReplicas)

      removedReplica.foreach { replica =>
        replicasToPartitionsMap.get(replica).foreach { replicaSet =>
          replicaSet.remove(topicPartition)
          if (replicaSet.isEmpty) {
            replicasToPartitionsMap.remove(replica)
          }
        }
      }

      addedReplicas.foreach { replica =>
        replicasToPartitionsMap.getOrElseUpdate(replica, mutable.Set.empty)
          .addOne(topicPartition)
      }
    }
  }

  def leaderFollowedBy(replica: Int): Set[TopicPartition] = {
    replicasToPartitionsMap.get(replica).map(_.toSet).getOrElse(Set.empty[TopicPartition])
  }
}
