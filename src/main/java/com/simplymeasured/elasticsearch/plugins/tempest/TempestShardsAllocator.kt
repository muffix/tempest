/*
 * The MIT License (MIT)
 * Copyright (c) 2016 DataRank, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 *
 */

package com.simplymeasured.elasticsearch.plugins.tempest

import com.simplymeasured.elasticsearch.plugins.tempest.balancer.*
import com.simplymeasured.elasticsearch.plugins.tempest.balancer.model.ModelNode
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.api.map.MapIterable
import org.eclipse.collections.impl.factory.Lists
import org.eclipse.collections.impl.factory.Maps
import org.elasticsearch.cluster.routing.ShardRouting
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.inject.Singleton
import org.elasticsearch.common.settings.Settings
import org.joda.time.DateTime
import java.util.*

/**
 * Tempest Shards Allocator that delegates to a Heuristic Balancer
 */

@Singleton
class TempestShardsAllocator
    @Inject constructor(settings: Settings,
                        val balancerConfiguration: BalancerConfiguration,
                        val indexGroupPartitioner: IndexGroupPartitioner,
                        val shardSizeCalculator: ShardSizeCalculator) :
        AbstractComponent(settings), ShardsAllocator {

    var lastRebalanceAttemptDateTime: DateTime = DateTime(0)
    var lastBalanceChangeDateTime: DateTime = DateTime(0)
    var lastOptimalBalanceFoundDateTime: DateTime = DateTime(0)
    var lastClusterBalanceScore: Double = 0.0
    var lastNodeGroupScores: MapIterable<String, MapIterable<String, Double>> = Maps.immutable.empty<String, MapIterable<String, Double>>()
    private var futurePreApprovedMoveDescriptionBatches: ListIterable<ListIterable<MoveDescription>> = Lists.immutable.empty()
    var status: String = "unknown"
    private var random: Random = Random()


    override fun allocate(allocation: RoutingAllocation) {
        if (allocation.routingNodes().size() == 0) { return }
        lastRebalanceAttemptDateTime = DateTime()

        buildBalancer(allocation).run {
            updateScoreStats()

            val decision = when (BalanceDecision.BALANCING) {
                allocateUnassigned() -> BalanceDecision.BALANCING
                moveShards() -> BalanceDecision.BALANCING
                rebalance() -> BalanceDecision.BALANCING
                else -> BalanceDecision.BALANCED
            }

            updateFutureMoveDescriptions(this, decision)
            updateStatus(decision)
        }
    }

    private fun updateFutureMoveDescriptions(balancer: HeuristicBalancer, balanceDecision: BalanceDecision) {
        when (balanceDecision) {
            BalanceDecision.BALANCING -> this.futurePreApprovedMoveDescriptionBatches = balancer.futurePreApprovedMoveDescriptionBatches
            BalanceDecision.BALANCED -> this.futurePreApprovedMoveDescriptionBatches = Lists.immutable.empty()
            BalanceDecision.ON_HOLD -> { /* do nothing */ }
            BalanceDecision.NO_OP -> { /* do nothing */ }
        }
    }

    override fun decideShardAllocation(shard: ShardRouting, allocation: RoutingAllocation): ShardAllocationDecision {
        throw UnsupportedOperationException("Tempest does not support shard allocation explanations at this time")
    }

    private fun updateStatus(balanceDecision: BalanceDecision): BalanceDecision {
        when (balanceDecision) {
            BalanceDecision.BALANCING -> {
                lastBalanceChangeDateTime = DateTime()
                status = "balancing"
            }

            BalanceDecision.ON_HOLD -> {
                status = "on hold"
            }

            BalanceDecision.BALANCED -> {
                lastOptimalBalanceFoundDateTime = DateTime()
                status = "balanced"
            }

            BalanceDecision.NO_OP -> { /* do nothing */ }
        }
        return balanceDecision
    }

    private fun buildBalancer(allocation: RoutingAllocation): HeuristicBalancer = HeuristicBalancer(
            settings = settings,
            allocation = allocation,
            shardSizeCalculator = shardSizeCalculator,
            balancerConfiguration = balancerConfiguration,
            preApprovedMoveDescriptionBatches = futurePreApprovedMoveDescriptionBatches,
            random = random)

    private fun HeuristicBalancer.updateScoreStats() {
        lastClusterBalanceScore = this.baseModelCluster.calculateBalanceScore()
        lastNodeGroupScores = this.baseModelCluster.modelNodes
                .toMap({ it.backingNode.node().hostName }, { buildScoreGroupSummary(it) })
    }

    private fun buildScoreGroupSummary(modelNode: ModelNode): MapIterable<String, Double> {
        return modelNode.shardManager.shardScoreGroupDetails
                .keyValuesView()
                .select { it.two.balanceScore != 0.0 || it.two.shards.notEmpty() }
                .toMap( {"${it.one.index} ${if (it.one.includesPrimaries) "p" else ""}${if (it.one.includesReplicas) "r" else ""}"}, {it.two.balanceScore})
    }
}



