/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.orderbook;

import exchange.core2.core.common.IOrder;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * 订单桶简单实现
 * 线程安全性需由外部保证（非线程安全实现）
 *
 * 关键设计特点
 *
 * 1.FIFO匹配保证
 * LinkedHashMap确保先入订单优先匹配，符合交易所公平性原则。
 *
 * 2.事件链生成
 * 匹配过程中动态构建MatcherTradeEvent链表，避免中间集合分配开销。
 *
 * 3.延迟删除
 * 匹配完成后返回ordersToRemove列表，由调用方统一处理订单移除。
 *
 * 4.防御性校验
 * validate()方法用于检测内部状态一致性（测试环境使用）。
 *
 */
@Slf4j
@ToString
public final class OrdersBucketNaive implements Comparable<OrdersBucketNaive>, WriteBytesMarshallable {

    // 当前订单桶的价格水平
    @Getter
    private final long price;
    /**
     * 订单存储（OrderId -> Order）
     * 确保先入订单优先匹配，符合交易所公平性原则
     */
    private final LinkedHashMap<Long, Order> entries;
    // 桶内未成交订单总成交量
    @Getter
    private long totalVolume;

    public OrdersBucketNaive(final long price) {
        this.price = price;
        this.entries = new LinkedHashMap<>();
        this.totalVolume = 0;
    }

    public OrdersBucketNaive(BytesIn bytes) {
        this.price = bytes.readLong();
        this.entries = SerializationUtils.readLongMap(bytes, LinkedHashMap::new, Order::new);
        this.totalVolume = bytes.readLong();
    }

    /**
     * 添加新订单
     *
     * Put a new order into bucket
     *
     * @param order - order
     */
    public void put(Order order) {
        // 1. 存入entries
        entries.put(order.orderId, order);
        // 2. 更新totalVolume += (order.size - order.filled)
        totalVolume += order.size - order.filled;
    }

    /**
     * 移除订单
     * Remove order from the bucket
     *
     * @param orderId - order id
     * @param uid     - order uid
     * @return order if removed, or null if not found
     */
    public Order remove(long orderId, long uid) {
        Order order = entries.get(orderId);
//        log.debug("removing order: {}", order);
        // 1. 校验订单ID与用户ID匹配
        if (order == null || order.uid != uid) {
            return null;
        }
        // 2.移除订单
        entries.remove(orderId);
        // 3. 更新totalVolume -= 剩余量
        totalVolume -= order.size - order.filled;
        // 4. 返回被移除订单
        return order;
    }

    /**
     * 订单匹配核心方法
     *
     * Collect a list of matching orders starting from eldest records
     * Completely matching orders will be removed, partially matched order kept in the bucked.
     *
     * 从最早订单开始匹配（FIFO），直到满足需成交量volumeToCollect或桶内订单耗尽
     *
     * @param volumeToCollect - volume to collect 需收集的成交量
     * @param activeOrder     - for getReserveBidPrice 主动买卖的订单,用于获取竞拍预订买入价格
     * @param helper          - events helper 事件助手
     *
     * @return - total matched volume, events, completed orders to remove // 交易事件链表头、尾，实际匹配总量，需移除的订单ID列表
     */
    public MatcherResult match(long volumeToCollect, IOrder activeOrder, OrderBookEventsHelper helper) {

//        log.debug("---- match: {}", volumeToCollect);

        final Iterator<Map.Entry<Long, Order>> iterator = entries.entrySet().iterator();

        // 总匹配量
        long totalMatchingVolume = 0;
        // 代删除订单列表
        final List<Long> ordersToRemove = new ArrayList<>();

        // 匹配交易事件头
        MatcherTradeEvent eventsHead = null;
        // 匹配交易事件尾
        MatcherTradeEvent eventsTail = null;

        // iterate through all orders
        // 1.遍历订单（LinkedHashMap迭代器保证顺序）
        while (iterator.hasNext() && volumeToCollect > 0) {
            final Map.Entry<Long, Order> next = iterator.next();
            final Order order = next.getValue();

            // calculate exact volume can fill for this order
//            log.debug("volumeToCollect={} order: s{} f{}", volumeToCollect, order.size, order.filled);
            // 计算当前订单可成交量：v = min(剩余需成交量, 订单剩余量)
            final long v = Math.min(volumeToCollect, order.size - order.filled);
            totalMatchingVolume += v;
//            log.debug("totalMatchingVolume={} v={}", totalMatchingVolume, v);
            // 订单已成交量order.filled += v
            order.filled += v;
            // 剩余需成交量volumeToCollect -= v
            volumeToCollect -= v;
            // 桶总剩余量totalVolume -= v
            reduceSize(v);
            // remove from order book filled orders
            // 完全成交标志
            final boolean fullMatch = order.size == order.filled;
            // 投标人持有价格,那个单是买单取那个单的报的买入价格
            final long bidderHoldPrice = order.action == OrderAction.ASK ? activeOrder.getReserveBidPrice() : order.reserveBidPrice;
            // 生成交易事件（MatcherTradeEvent）
            final MatcherTradeEvent tradeEvent = helper.sendTradeEvent(order, fullMatch, volumeToCollect == 0, v, bidderHoldPrice);

            if (eventsTail == null) {
                // 链尾为空,链表还未开始构建,当前事件节点为链头
                eventsHead = tradeEvent;
            } else {
                // 当前事件链接到原链尾
                eventsTail.nextEvent = tradeEvent;
            }
            // 更新链尾节点为当前事件节点
            eventsTail = tradeEvent;

            // 完全成交的订单移入ordersToRemove列表
            if (fullMatch) {
                ordersToRemove.add(order.orderId);
                iterator.remove();
            }
        }

        return new MatcherResult(eventsHead, eventsTail, totalMatchingVolume, ordersToRemove);
    }

    /**
     *
     * 获取订单数量
     * Get number of orders in the bucket
     *
     * @return number of orders in the bucket
     */
    public int getNumOrders() {
        return entries.size();
    }

    /**
     * 减少订单总未成交量
     *
     * Reduce size of the order
     *
     * @param reduceSize - size to reduce (difference)
     */
    public void reduceSize(long reduceSize) {
        // 直接扣减totalVolume（用于部分成交后调整）
        totalVolume -= reduceSize;
    }

    /**
     * 校验totalVolume与订单实际剩余量一致性（调试用）
     */
    public void validate() {
        long sum = entries.values().stream().mapToLong(c -> c.size - c.filled).sum();
        if (sum != totalVolume) {
            String msg = String.format("totalVolume=%d calculated=%d", totalVolume, sum);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * 查找订单
     *
     * @param orderId 订单号
     *
     * @return Order实例
     */
    public Order findOrder(long orderId) {
        return entries.get(orderId);
    }

    /**
     * 返回桶内所有订单（按插入顺序）
     * 标注为低效方法（深拷贝），仅用于测试
     * Inefficient method - for testing only
     *
     * @return new array with references to orders, preserving execution queue order
     */
    public List<Order> getAllOrders() {
        return new ArrayList<>(entries.values());
    }


    /**
     * 遍历所有订单执行操作
     *
     * execute some action for each order (preserving execution queue order)
     *
     * @param consumer action consumer function
     */
    public void forEachOrder(Consumer<Order> consumer) {
        entries.values().forEach(consumer);
    }

    /**
     * 转为单行显示
     *
     * 生成订单桶文本快照（调试用）
     *
     * @return 单行快照字符串
     */
    public String dumpToSingleLine() {
        String orders = getAllOrders().stream()
                .map(o -> String.format("id%d_L%d_F%d", o.orderId, o.size, o.filled))
                .collect(Collectors.joining(", "));

        return String.format("%d : vol:%d num:%d : %s", getPrice(), getTotalVolume(), getNumOrders(), orders);
    }

    /**
     * 写入价格、订单Map、总成交量
     *
     * @param bytes to write to.
     */
    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeLong(price);
        SerializationUtils.marshallLongMap(entries, bytes);
        bytes.writeLong(totalVolume);
    }

    /**
     * 比较
     *
     * 按价格排序
     *
     * @param other the object to be compared.
     *
     */
    @Override
    public int compareTo(OrdersBucketNaive other) {
        return Long.compare(this.getPrice(), other.getPrice());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                price,
                Arrays.hashCode(entries.values().toArray(new Order[0])));
    }

    /**
     * 相等判断
     *
     * 要求价格相同且所有订单一致
     *
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof OrdersBucketNaive)) return false;
        OrdersBucketNaive other = (OrdersBucketNaive) o;
        return price == other.getPrice()
                && getAllOrders().equals(other.getAllOrders());
    }


    @AllArgsConstructor
    public final class MatcherResult {
        /**
         * 事件链头
         */
        public MatcherTradeEvent eventsChainHead;
        /**
         * 事件链尾
         */
        public MatcherTradeEvent eventsChainTail;
        /**
         * 成交量
         */
        public long volume;
        /**
         * 待删除的订单ID列表
         */
        public List<Long> ordersToRemove;
    }

}
