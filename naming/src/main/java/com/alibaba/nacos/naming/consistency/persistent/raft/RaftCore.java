/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * @author nacos
 */
@Component
public class RaftCore {

    /**
     * POST HTTP://{ip:port}/v1/ns/raft/vote : 进行投票请求
     */
    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    /**
     * POST HTTP://{ip:port}/v1/ns/raft/beat : Leader向Follower发送心跳信息
     */
    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    /**
     * POST HTTP://{ip:port}/v1/ns/raft/datum : Leader接收传来的数据并存入
     */
    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    /**
     * DELETE HTTP://{ip:port}/v1/ns/raft/datum : Leader接收传来的数据删除操作
     */
    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    /**
     * GET HTTP://{ip:port}/v1/ns/raft/datum : 获取该节点存储的数据信息
     */
    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    /**
     * POST HTTP://{ip:port}/v1/ns/raft/datum/commit : Follower节点接收Leader传来得到数据存入操作
     */
    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    /**
     * DELETE HTTP://{ip:port}/v1/ns/raft/datum : Follower节点接收Leader传来的数据删除操作
     */
    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    /**
     * GET HTTP://{ip:port}/v1/ns/raft/peer : 获取该节点的RaftPeer信息
     */
    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.raft.notifier");

            return t;
        }
    });

    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

    private volatile Map<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    /**
     * 参与者集合
     */
    @Autowired
    private RaftPeerSet peers;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 全局参数
     */
    @Autowired
    private GlobalConfig globalConfig;

    /**
     * raft 代理
     */
    @Autowired
    private RaftProxy raftProxy;

    /**
     * raft存储
     */
    @Autowired
    private RaftStore raftStore;

    /**
     * 通知器
     */
    public volatile Notifier notifier = new Notifier();

    private boolean initialized = false;

    @PostConstruct
    public void init() throws Exception {

        Loggers.RAFT.info("initializing Raft sub-system");

        executor.submit(notifier);

        long start = System.currentTimeMillis();

        // 进行日志文件的加载到内存数据对象Datums的操作
        raftStore.loadDatums(notifier, datums);

        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

        // 等待上一步的数据加载任务全部完成
        while (true) {
            if (notifier.tasks.size() <= 0) {
                break;
            }
            Thread.sleep(1000L);
        }

        initialized = true;

        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

        // 500ms 调度一次选举
        GlobalExecutor.registerMasterElection(new MasterElection());

        // 500ms 发送一次心跳
        GlobalExecutor.registerHeartbeat(new HeartBeat());

        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
            GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, List<RecordListener>> getListeners() {
        return listeners;
    }

    public void signalPublish(String key, Record value) throws Exception {

        // 如果不是leader，则把数据发送给leader
        if (!isLeader()) {
            JSONObject params = new JSONObject();
            params.put("key", key);
            params.put("value", value);
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);

            raftProxy.proxyPostLarge(getLeader().ip, API_PUB, params.toJSONString(), parameters);
            return;
        }

        // 如果是leader
        try {
            OPERATE_LOCK.lock();
            long start = System.currentTimeMillis();
            // 设置Datum，timestamp 为序列时间戳
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            if (getDatum(key) == null) {
                datum.timestamp.set(1L);
            } else {
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }

            // 发送 数据事件，
            JSONObject json = new JSONObject();
            json.put("datum", datum);
            json.put("source", peers.local());

            onPublish(datum, peers.local());

            final String content = JSON.toJSONString(json);

            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) {
                    latch.countDown();
                    continue;
                }
                final String url = buildURL(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key=" + key), content, new AsyncCompletionHandler<Integer>() {
                    @Override
                    public Integer onCompleted(Response response) throws Exception {
                        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                            Loggers.RAFT.warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                datum.key, server, response.getStatusCode());
                            return 1;
                        }
                        latch.countDown();
                        return 0;
                    }

                    @Override
                    public STATE onContentWriteCompleted() {
                        return STATE.CONTINUE;
                    }
                });

            }

            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }

            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    public void signalDelete(final String key) throws Exception {

        OPERATE_LOCK.lock();
        try {

            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            JSONObject json = new JSONObject();
            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            json.put("datum", datum);
            json.put("source", peers.local());

            onDelete(datum.key, peers.local());

            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildURL(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, JSON.toJSONString(json)
                    , new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}", key, server, response.getStatusCode());
                                return 1;
                            }

                            RaftPeer local = peers.local();

                            local.resetLeaderDue();

                            return 0;
                        }
                    });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " +
                "data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term.get() + ", cur-term: " + local.term.get());
        }

        local.resetLeaderDue();

        // if data should be persistent, usually this is always true:
        if (KeyBuilder.matchPersistentKey(datum.key)) {
            raftStore.write(datum);
        }

        datums.put(datum.key, datum);

        if (isLeader()) {
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        raftStore.updateTerm(local.term.get());

        notifier.addTask(datum.key, ApplyAction.CHANGE);

        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    public void onDelete(String datumKey, RaftPeer source) throws Exception {

        RaftPeer local = peers.local();

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term + ", cur-term: " + local.term);
        }

        local.resetLeaderDue();

        // do apply
        String key = datumKey;
        deleteDatum(key);

        if (KeyBuilder.matchServiceMetaKey(key)) {

            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }

            raftStore.updateTerm(local.term.get());
        }

        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

    }

    public class MasterElection implements Runnable {
        @Override
        public void run() {
            try {
                // 是否准备好选举，没有就退出
                if (!peers.isReady()) {
                    return;
                }

                // 获取本地 peer
                RaftPeer local = peers.local();

                // 本地存储的Leader任期时间
                // 总 15000ms，每次调度减少 500ms
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                //如果Leader任期时间还在允许范围内，则不进行Leader选举
                if (local.leaderDueMs > 0) {
                    return;
                }
                //如果15s钟内，没有递延任期，则进行重新选举（在收到leader的心跳时，会递延leader任期时间）

                // reset timeout
                // 重置Leader任期时间
                local.resetLeaderDue();
                //重置本地心跳任期时间
                local.resetHeartbeatDue();

                // 向其他节点发起投票请求
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }

        }

        public void sendVote() {

            //获取本地peer
            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}",
                JSON.toJSONString(getLeader()), local.term);

            // 重置所有到选票为空
            peers.reset();

            //任期加1
            local.term.incrementAndGet();
            // 本地选举为自己
            local.voteFor = local.ip;
            // 状态改为候选人
            local.state = RaftPeer.State.CANDIDATE;

            //参数 投票为自己
            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JSON.toJSONString(local));

            for (final String server : peers.allServersWithoutMySelf()) {
                final String url = buildURL(server, API_VOTE);
                try {
                    HttpClient.asyncHttpPost(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            //如果返回非ok，则返回失败
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", response.getResponseBody(), url);
                                return 1;
                            }

                            RaftPeer peer = JSON.parseObject(response.getResponseBody(), RaftPeer.class);

                            Loggers.RAFT.info("received approve from peer: {}", JSON.toJSONString(peer));
                            // 获取投票结果，并进行Leader的选举工作
                            peers.decideLeader(peer);

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

     // 其他节点接收到投票请求后的反应
    public RaftPeer receivedVote(RaftPeer remote) {
        // 被选举人是否在raft集群节点列表中
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }

        // 获取本地 peer
        RaftPeer local = peers.get(NetUtils.localServer());
        // 如果被选举节点的周期数小于本节点的周期数，则将自己的投票投给自己并告诉被选举者
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" +
                ", voter-term:" + remote.term + ", votee-term:" + local.term;

            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;
            }

            return local;
        }

        // 重置Leader任期时间
        local.resetLeaderDue();

        //否则，本地投票给远程对象
        local.state = RaftPeer.State.FOLLOWER;
        local.voteFor = remote.ip;
        local.term.set(remote.term.get());

        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        return local;
    }

    public class HeartBeat implements Runnable {
        @Override
        public void run() {
            try {
                // 程序是否已准备完毕
                if (!peers.isReady()) {
                    return;
                }

                // 心跳一次减少500ms，整体5000ms
                RaftPeer local = peers.local();
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                // 心跳周期判断
                if (local.heartbeatDueMs > 0) {
                    return;
                }

                // 重置心跳周期
                local.resetHeartbeatDue();
                // 发送心跳信息
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }

        }

        //发送心跳
        public void sendBeat() throws IOException, InterruptedException {
            // 获取本地 peer
            RaftPeer local = peers.local();
            // 如果是集群模式，不是leader，则退出
            if (local.state != RaftPeer.State.LEADER && !STANDALONE_MODE) {
                return;
            }

            // 如果是集群模式，是leader。
            Loggers.RAFT.info("[RAFT] send beat with {} keys.", datums.size());

            // 重置Leader任期时间（递延任期时间）
            local.resetLeaderDue();

            // build data（生成数据）
            JSONObject packet = new JSONObject();
            packet.put("peer", local);

            JSONArray array = new JSONArray();

            // 只发送心跳消息
            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", String.valueOf(switchDomain.isSendBeatOnly()));
            }

            // 如果开启了在心跳包中携带Leader存储的数据进行发送，则对数据进行打包操作
            if (!switchDomain.isSendBeatOnly()) {
                for (Datum datum : datums.values()) {
                    // 只发送 key 和 timestamp
                    JSONObject element = new JSONObject();
                    // 服务元数据
                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));

                        // iplist数据
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp);

                    array.add(element);
                }
            } else {
                Loggers.RAFT.info("[RAFT] send beat only.");
            }

            // 数据
            packet.put("datums", array);
            // broadcast
            Map<String, String> params = new HashMap<String, String>(1);
            //心跳数据
            params.put("beat", JSON.toJSONString(packet));

            String content = JSON.toJSONString(params);

            // 将参数信息进行 Gzip算法压缩，降低网络消耗
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);
            Loggers.RAFT.info("raw beat data size: {}, size of compressed data: {}",
                content.length(), compressedContent.length());

            // 给follower发送心跳数据
            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    final String url = buildURL(server, API_BEAT);
                    Loggers.RAFT.info("send beat to server " + server);
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            // 如果失败 心跳发送失败次数加1
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}",
                                    response.getResponseBody(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return 1;
                            }

                            // 成功后接收Follower节点的心跳回复(Follower节点的当前信息)进行节点更新操作
                            peers.update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));
                            Loggers.RAFT.info("receive beat response from: {}", url);
                            return 0;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            // 如果失败 心跳发送失败次数加1
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server, t);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }
                    });
                } catch (Exception e) {
                    // 如果失败 心跳发送失败次数加1
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }

        }
    }

    /**
     * 收到心跳数据的处理
     *
     * @param beat
     * @return
     * @throws Exception
     */
    public RaftPeer receivedBeat(JSONObject beat) throws Exception {
        // 本地 peer
        final RaftPeer local = peers.local();

        // 远程 peer
        final RaftPeer remote = new RaftPeer();
        remote.ip = beat.getJSONObject("peer").getString("ip");
        remote.state = RaftPeer.State.valueOf(beat.getJSONObject("peer").getString("state"));
        remote.term.set(beat.getJSONObject("peer").getLongValue("term"));
        remote.heartbeatDueMs = beat.getJSONObject("peer").getLongValue("heartbeatDueMs");
        remote.leaderDueMs = beat.getJSONObject("peer").getLongValue("leaderDueMs");
        remote.voteFor = beat.getJSONObject("peer").getString("voteFor");

        //如果不是leader 发送的 抛出异常
        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}",
                remote.state, JSON.toJSONString(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }

        // 如果本地 任期大于远程的抛出异常
        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT.info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}"
                , remote.term.get(), local.term.get(), JSON.toJSONString(remote), local.leaderDueMs);
            throw new IllegalArgumentException("out of date beat, beat-from-term: " + remote.term.get()
                + ", beat-to-term: " + local.term.get());
        }

        // local 如果不是 follower 则 指定远程为leader
        if (local.state != RaftPeer.State.FOLLOWER) {

            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JSON.toJSONString(remote));
            // mk follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }

        final JSONArray beatDatums = beat.getJSONArray("datums");
        // 如果收到 心跳消息，则重置任期时间（递延任期时间）
        local.resetLeaderDue();
        local.resetHeartbeatDue();

        // 指定远程为leader
        peers.makeLeader(remote);

        // 远程的keys
        Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());

        // 记录心跳包，发过来的leader的 key
        for (Map.Entry<String, Datum> entry : datums.entrySet()) {
            receivedKeysMap.put(entry.getKey(), 0);
        }

        // now check datums
        List<String> batch = new ArrayList<>();

        //如果不仅仅是发送心跳消息
        if (!switchDomain.isSendBeatOnly()) {
            int processedCount = 0;
            Loggers.RAFT.info("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            for (Object object : beatDatums) {
                // 获取数据
                processedCount = processedCount + 1;

                JSONObject entry = (JSONObject) object;
                String key = entry.getString("key");
                final String datumKey;
                // 构建 datumKey（加上前缀，发送的时候 key 是去掉了前缀的）
                if (KeyBuilder.matchServiceMetaKey(key)) {
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // ignore corrupted key:
                    continue;
                }
                 // 获取收到的 key 对应的版本
                long timestamp = entry.getLong("timestamp");
                // 将收到的 key 在本地 key 的 map 中标记为 1
                receivedKeysMap.put(datumKey, 1);

                try {
                    // 收到的 key 在本地存在 并且 本地的版本大于收到的版本 并且 还有数据未处理，则直接 continue
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp && processedCount < beatDatums.size()) {
                        continue;
                    }
                    // 若收到的 key 在本地没有，或者本地的版本小于收到的版本，放入 batch，准备下一步获取数据
                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        batch.add(datumKey);
                    }

                    // 只有 batch 的数量超过 50 或已经处理完了，才进行获取数据操作
                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }

                    String keys = StringUtils.join(batch, ",");

                    if (batch.size() <= 0) {
                        continue;
                    }

                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}, datums' size is {}, RaftCore.datums' size is {}"
                        , getLeader().ip, batch.size(), processedCount, beatDatums.size(), datums.size());

                    // update datum entry
                    // 获取远程 数据
                    String url = buildURL(remote.ip, API_GET) + "?keys=" + URLEncoder.encode(keys, "UTF-8");
                    HttpClient.asyncHttpGet(url, null, null, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                return 1;
                            }

                            List<JSONObject> datumList = JSON.parseObject(response.getResponseBody(), new TypeReference<List<JSONObject>>() {
                            });

                            // 更新本地数据
                            for (JSONObject datumJson : datumList) {
                                OPERATE_LOCK.lock();
                                Datum newDatum = null;
                                try {

                                    Datum oldDatum = getDatum(datumJson.getString("key"));

                                    if (oldDatum != null && datumJson.getLongValue("timestamp") <= oldDatum.timestamp.get()) {
                                        Loggers.RAFT.info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                            datumJson.getString("key"), datumJson.getLongValue("timestamp"), oldDatum.timestamp);
                                        continue;
                                    }

                                    if (KeyBuilder.matchServiceMetaKey(datumJson.getString("key"))) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.getString("key");
                                        serviceDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        serviceDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Service.class);
                                        newDatum = serviceDatum;
                                    }

                                    if (KeyBuilder.matchInstanceListKey(datumJson.getString("key"))) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.getString("key");
                                        instancesDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        instancesDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Instances.class);
                                        newDatum = instancesDatum;
                                    }

                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }

                                    raftStore.write(newDatum);

                                    datums.put(newDatum.key, newDatum);
                                    notifier.addTask(newDatum.key, ApplyAction.CHANGE);

                                    local.resetLeaderDue();

                                    // 如果本地和远程任期差距不大，直接设置为远程任期，差距大，每次增加100
                                    if (local.term.get() + 100 > remote.term.get()) {
                                        getLeader().term.set(remote.term.get());
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        local.term.addAndGet(100);
                                    }

                                    raftStore.updateTerm(local.term.get());

                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                        newDatum.key, newDatum.timestamp, JSON.toJSONString(remote), local.term);

                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum, e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            TimeUnit.MILLISECONDS.sleep(200);
                            return 0;
                        }
                    });

                    batch.clear();

                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }

            }

            List<String> deadKeys = new ArrayList<>();
            // 若某个 key 在本地存在但收到的 key 列表中没有，则证明 leader 已经删除，那么本地也要删除
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }

            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }

        }

        return local;
    }

    public void listen(String key, RecordListener listener) {

        List<RecordListener> listenerList = listeners.get(key);
        if (listenerList != null && listenerList.contains(listener)) {
            return;
        }

        if (listenerList == null) {
            listenerList = new CopyOnWriteArrayList<>();
            listeners.put(key, listenerList);
        }

        Loggers.RAFT.info("add listener: {}", key);

        listenerList.add(listener);

        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    public void unlisten(String key, RecordListener listener) {

        if (!listeners.containsKey(key)) {
            return;
        }

        for (RecordListener dl : listeners.get(key)) {
            // TODO maybe use equal:
            if (dl == listener) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    public void unlistenAll(String key) {
        listeners.remove(key);
    }

    public void setTerm(long term) {
        peers.setTerm(term);
    }

    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    public static String buildURL(String ip, String api) {
        if (!ip.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
            ip = ip + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
        }
        return "http://" + ip + RunningConfig.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    public int datumSize() {
        return datums.size();
    }

    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        notifier.addTask(datum.key, ApplyAction.CHANGE);
    }

    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }

    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            notifier.addTask(URLDecoder.decode(key, "UTF-8"), ApplyAction.DELETE);
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    public int getNotifyTaskCount() {
        return notifier.getTaskSize();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<>(1024 * 1024);

        public void addTask(String datumKey, ApplyAction action) {

            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            if (action == ApplyAction.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }

            Loggers.RAFT.info("add task {}", datumKey);

            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.RAFT.info("raft notifier started");

            while (true) {
                try {

                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    services.remove(datumKey);

                    Loggers.RAFT.info("remove task {}", datumKey);

                    int count = 0;

                    if (listeners.containsKey(KeyBuilder.SERVICE_META_KEY_PREFIX)) {

                        if (KeyBuilder.matchServiceMetaKey(datumKey) && !KeyBuilder.matchSwitchKey(datumKey)) {

                            for (RecordListener listener : listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX)) {
                                try {
                                    if (action == ApplyAction.CHANGE) {
                                        listener.onChange(datumKey, getDatum(datumKey).value);
                                    }

                                    if (action == ApplyAction.DELETE) {
                                        listener.onDelete(datumKey);
                                    }
                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                                }
                            }
                        }
                    }

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            if (action == ApplyAction.CHANGE) {
                                listener.onChange(datumKey, getDatum(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("[NACOS-RAFT] datum change notified, key: {}, listener count: {}", datumKey, count);
                    }
                } catch (Throwable e) {
                    Loggers.RAFT.error("[NACOS-RAFT] Error while handling notifying task", e);
                }
            }
        }
    }
}
