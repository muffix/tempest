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

import junit.framework.TestCase
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs
import org.eclipse.collections.impl.factory.Lists
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESIntegTestCase
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.security.AccessController
import java.security.PrivilegedAction

/**
 * Created by awhite on 4/15/16.
 */
class TempestShardsAllocatorITests : ESIntegTestCase() {
    private val runner = ElasticsearchClusterRunner()

    @Before
    @Throws(Exception::class)
    override fun setUp() {
        super.setUp()
        runner.onBuild { _, settingsBuilder ->
            settingsBuilder.put("logger.com.simplymeasured.elasticsearch.plugins.tempest", "DEBUG")
            settingsBuilder.put("logger.org.elasticsearch.cluster.routing.allocation", "DEBUG")
            settingsBuilder.put("tempest.balancer.groupingPatterns", "index-\\w+,index-\\w+-\\d+")

            settingsBuilder.put("cluster.routing.allocation.type", "tempest")
            settingsBuilder.put("cluster.routing.allocation.same_shard.host", false)
            settingsBuilder.put("http.cors.enabled", true)
            settingsBuilder.put("http.cors.allow-origin", "*")
            settingsBuilder.put("cluster.routing.allocation.disk.watermark.low", "95%")
            settingsBuilder.put("cluster.routing.allocation.disk.watermark.high", "99%")
            settingsBuilder.put("cluster.routing.allocation.disk.watermark.flood_stage", "99%")
        }.build(newConfigs()
                .numOfNode(5)
                .pluginTypes("com.simplymeasured.elasticsearch.plugins.tempest.TempestPlugin")
        )

        runner.ensureGreen()
    }

    @After
    override fun tearDown() {
        runner.close()
        runner.clean()
        super.tearDown()
    }

    @Test
    @Throws(Exception::class)
    fun testTempestShardsAllocator() {
        runner.createIndex("index-a", Settings.builder().put("index.number_of_replicas", "2").build())
        runner.createIndex("index-b", Settings.builder().put("index.number_of_replicas", "2").build())
        runner.createIndex("index-c-123", Settings.builder().put("index.number_of_replicas", "2").build())

        runner.ensureGreen("index-a", "index-b", "index-c-123")

        while (true) {
        }
    }
}
