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

import exchange.core2.collections.art.LongAdaptiveRadixTreeMap;
import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.LoggingConfiguration;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 订单薄直接实现
 */
@Slf4j
public final class OrderBookDirectImpl implements IOrderBook {

    // buckets 每个桶（Bucket）维护一个订单链表，链表的尾部（tail）是价格最优的订单（对于卖单是价格最低，对于买单是价格最高）
    /**
     * 卖单订单薄
     */
    private final LongAdaptiveRadixTreeMap<Bucket> askPriceBuckets;
    /**
     * 买单订单薄
     */
    private final LongAdaptiveRadixTreeMap<Bucket> bidPriceBuckets;

    // symbol specification
    /**
     * 币种说明
     */
    private final CoreSymbolSpecification symbolSpec;

    // index: orderId -> order
    /**
     * 作为订单索引，便于通过订单ID快速查找订单
     */
    private final LongAdaptiveRadixTreeMap<DirectOrder> orderIdIndex;
//    private final Long2ObjectHashMap<DirectOrder> orderIdIndex = new Long2ObjectHashMap<>();
//    private final LongObjectHashMap<DirectOrder> orderIdIndex = new LongObjectHashMap<>();

    // heads (nullable) 维护两个指针：bestAskOrder和bestBidOrder，分别指向当前最优的卖单和买单（即卖单最低价，买单最高价）
    /**
     * 最优卖单
     */
    private DirectOrder bestAskOrder = null;
    /**
     * 最优买单
     */
    private DirectOrder bestBidOrder = null;

    // Object pools
    /**
     * 使用对象池（ObjectsPool）来重用对象，减少GC压力
     */
    private final ObjectsPool objectsPool;

    /**
     * 订单薄事件助手
     */
    private final OrderBookEventsHelper eventsHelper;

    /**
     * 启用调试
     */
    private final boolean logDebug;

    /**
     * 构造函数
     *
     * @param symbolSpec 币种描述符
     * @param objectsPool 对象池
     * @param eventsHelper 事件助手
     * @param loggingCfg 日志配置
     */
    public OrderBookDirectImpl(final CoreSymbolSpecification symbolSpec,
                               final ObjectsPool objectsPool,
                               final OrderBookEventsHelper eventsHelper,
                               final LoggingConfiguration loggingCfg) {

        this.symbolSpec = symbolSpec;
        this.objectsPool = objectsPool;
        this.askPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.bidPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.eventsHelper = eventsHelper;
        this.orderIdIndex = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);
    }

    /**
     * 构造函数
     *
     * @param bytes 二进制数据输入
     * @param objectsPool 对线池
     * @param eventsHelper 事件助手
     * @param loggingCfg 日志配置
     */
    public OrderBookDirectImpl(final BytesIn bytes,
                               final ObjectsPool objectsPool,
                               final OrderBookEventsHelper eventsHelper,
                               final LoggingConfiguration loggingCfg) {

        this.symbolSpec = new CoreSymbolSpecification(bytes);
        this.objectsPool = objectsPool;
        this.askPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.bidPriceBuckets = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.eventsHelper = eventsHelper;
        this.orderIdIndex = new LongAdaptiveRadixTreeMap<>(objectsPool);
        this.logDebug = loggingCfg.getLoggingLevels().contains(LoggingConfiguration.LoggingLevel.LOGGING_MATCHING_DEBUG);

        final int size = bytes.readInt();
        for (int i = 0; i < size; i++) {
            DirectOrder order = new DirectOrder(bytes);
            insertOrder(order, null);
            orderIdIndex.put(order.orderId, order);
        }
    }

    /**
     * 添加新订单
     *
     * @param cmd - 订单指令 order to match/place
     */
    @Override
    public void newOrder(final OrderCommand cmd) {
        switch (cmd.orderType) {
            case GTC:
                newOrderPlaceGtc(cmd);
                break;
            case IOC:
                newOrderMatchIoc(cmd);
                break;
            case FOK_BUDGET:
                newOrderMatchFokBudget(cmd);
                break;
            // TODO IOC_BUDGET and FOK support
            default:
                log.warn("Unsupported order type: {}", cmd);
                eventsHelper.attachRejectEvent(cmd, cmd.size);
        }
    }

    /**
     * 取消订单
     *
     * @param cmd - order command
     *
     * @return CommandResultCode
     */
    @Override
    public CommandResultCode cancelOrder(OrderCommand cmd) {

        // TODO avoid double lookup ?
        final DirectOrder order = orderIdIndex.get(cmd.orderId);
        if (order == null || order.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }
        orderIdIndex.remove(cmd.orderId);
        objectsPool.put(ObjectsPool.DIRECT_ORDER, order);

        final Bucket freeBucket = removeOrder(order);
        if (freeBucket != null) {
            objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
        }

        // fill action fields (for events handling)
        cmd.action = order.getAction();

        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, order.getSize() - order.getFilled(), true);

        return CommandResultCode.SUCCESS;
    }

    /**
     * 缩减订单数量
     *
     * @param cmd - 订单指令 order command
     *
     * @return CommandResultCode
     */
    @Override
    public CommandResultCode reduceOrder(OrderCommand cmd) {
        // 获取订单ID和请求减少的数量
        final long orderId = cmd.orderId;
        final long requestedReduceSize = cmd.size;
        // 检查请求减少的数量是否合法
        if (requestedReduceSize <= 0) {
            return CommandResultCode.MATCHING_REDUCE_FAILED_WRONG_SIZE;
        }
        // 根据订单ID获取订单对象
        final DirectOrder order = orderIdIndex.get(orderId);
        // 检查订单是否存在以及订单的拥有者是否与指令匹配
        if (order == null || order.uid != cmd.uid) {
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }
        // 计算订单剩余的数量和实际需要减少的数量
        final long remainingSize = order.size - order.filled;
        final long reduceBy = Math.min(remainingSize, requestedReduceSize);
        // 判断是否可以移除订单
        final boolean canRemove = reduceBy == remainingSize;
        if (canRemove) {
            // 如果可以移除订单，则从索引中移除订单并回收对象
            orderIdIndex.remove(orderId);
            objectsPool.put(ObjectsPool.DIRECT_ORDER, order);
            final Bucket freeBucket = removeOrder(order);
            if (freeBucket != null) {
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
            }

        } else {
            // 如果不能移除订单，则更新订单的量和所属桶的量
            order.size -= reduceBy;
            order.parent.volume -= reduceBy;
        }
        // 发送缩减订单的事件
        cmd.matcherEvent = eventsHelper.sendReduceEvent(order, reduceBy, canRemove);
        // fill action fields (for events handling)
        // 填充指令的动作字段（用于事件处理）
        cmd.action = order.getAction();
        // 返回成功状态码
        return CommandResultCode.SUCCESS;
    }

    /**
     * 移动订单(改价)
     *
     * <p>
     * 举例来说，如果有一个订单ID为123，价格为100，用户ID为456的订单需要移动到新的价格位置。
     * 首先会通过orderIdIndex.get(cmd.orderId)查找该订单，如果找到订单并且用户ID匹配，则进行风险检查。
     * 如果风险检查通过，则移除该订单，并将其价格更新为新的命令价格。
     * 然后尝试立即匹配新的价格，如果订单完全匹配，则从订单ID索引中移除，并将订单对象返回到对象池中。
     * 如果订单没有完全匹配，则将其填充量更新，并插入到新的价格位置。最后返回成功命令结果码
     * </p>
     *
     * @param cmd - 订单指令 order command
     *
     * @return CommandResultCode
     */
    @Override
    public CommandResultCode moveOrder(OrderCommand cmd) {

        // order lookup 根据订单ID查找订单
        final DirectOrder orderToMove = orderIdIndex.get(cmd.orderId);
        if (orderToMove == null || orderToMove.uid != cmd.uid) {
            // // 如果订单不存在或订单的用户ID与命令的用户ID不匹配，返回未知订单ID的命令结果码
            return CommandResultCode.MATCHING_UNKNOWN_ORDER_ID;
        }

        // risk check for exchange bids 对于交易所的投标，进行风险检查
        if (symbolSpec.type == SymbolType.CURRENCY_EXCHANGE_PAIR && orderToMove.action == OrderAction.BID && cmd.price > orderToMove.reserveBidPrice) {
            // 如果命令价格超过订单的保留投标价格，返回价格超过风险限制的命令结果码
            return CommandResultCode.MATCHING_MOVE_FAILED_PRICE_OVER_RISK_LIMIT;
        }

        // remove order 移除订单
        final Bucket freeBucket = removeOrder(orderToMove);

        // update price 更新订单价格
        orderToMove.price = cmd.price;

        // fill action fields (for events handling) 填写动作字段（用于事件处理）
        cmd.action = orderToMove.getAction();

        // try match with new price as a taker order 使用新的价格尝试立即匹配
        final long filled = tryMatchInstantly(orderToMove, cmd);
        if (filled == orderToMove.size) {
            // order was fully matched - removing 如果订单完全匹配，从订单ID索引中移除
            orderIdIndex.remove(cmd.orderId);
            // returning free object back to the pool 将对象返回到对象池中
            objectsPool.put(ObjectsPool.DIRECT_ORDER, orderToMove);
            // 返回成功命令结果码
            return CommandResultCode.SUCCESS;
        }

        // not filled completely, inserting into new position 如果订单没有完全填充，将其插入到新的位置
        orderToMove.filled = filled;
        // insert into a new place 插入到一个新的价格
        insertOrder(orderToMove, freeBucket);

        return CommandResultCode.SUCCESS;
    }

    /**
     *
     * 统计订单数
     *
     * @param action 订单类型
     *
     * @return int
     */
    @Override
    public int getOrdersNum(OrderAction action) {
        final LongAdaptiveRadixTreeMap<Bucket> buckets = action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
        final MutableInteger accum = new MutableInteger();
        buckets.forEach((p, b) -> accum.value += b.numOrders, Integer.MAX_VALUE);
        return accum.value;
    }

    /**
     * 统计量
     *
     * @param action 订单类型
     *
     * @return long
     */
    @Override
    public long getTotalOrdersVolume(OrderAction action) {
        final LongAdaptiveRadixTreeMap<Bucket> buckets = action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
        final MutableLong accum = new MutableLong();
        buckets.forEach((p, b) -> accum.value += b.volume, Integer.MAX_VALUE);
        return accum.value;
    }

    /**
     * 获取订单
     *
     * @param orderId 订单ID
     *
     * @return IOrder
     */
    @Override
    public IOrder getOrderById(final long orderId) {
        return orderIdIndex.get(orderId);
    }

    /**
     * 验证快照状态
     */
    @Override
    public void validateInternalState() {
        final Long2ObjectHashMap<DirectOrder> ordersInChain = new Long2ObjectHashMap<>(orderIdIndex.size(Integer.MAX_VALUE), 0.8f);
        validateChain(true, ordersInChain);
        validateChain(false, ordersInChain);
//        log.debug("ordersInChain={}", ordersInChain);
//        log.debug("orderIdIndex={}", orderIdIndex);

//        log.debug("orderIdIndex.keySet()={}", orderIdIndex.keySet().toSortedArray());
//        log.debug("ordersInChain=        {}", ordersInChain.toSortedArray());
        orderIdIndex.forEach((k, v) -> {
            if (ordersInChain.remove(k) != v) {
                thrw("chained orders does not contain orderId=" + k);
            }
        }, Integer.MAX_VALUE);

        if (ordersInChain.size() != 0) {
            thrw("orderIdIndex does not contain each order from chains");
        }
    }

    /**
     * 获取订单薄实现类型
     *
     * @return OrderBookImplType
     */
    @Override
    public OrderBookImplType getImplementationType() {
        return OrderBookImplType.DIRECT;
    }

    /**
     * 查询用户的全部订单
     *
     * @param uid user id
     *
     * @return List<Order>
     */
    @Override
    public List<Order> findUserOrders(long uid) {
        final List<Order> list = new ArrayList<>();
        orderIdIndex.forEach((orderId, order) -> {
            if (order.uid == uid) {
                list.add(Order.builder()
                        .orderId(orderId)
                        .price(order.price)
                        .size(order.size)
                        .filled(order.filled)
                        .reserveBidPrice(order.reserveBidPrice)
                        .action(order.action)
                        .uid(order.uid)
                        .timestamp(order.timestamp)
                        .build());
            }
        }, Integer.MAX_VALUE);

        return list;
    }

    @Override
    public CoreSymbolSpecification getSymbolSpec() {
        return symbolSpec;
    }

    @Override
    public Stream<DirectOrder> askOrdersStream(boolean sortedIgnore) {
        return StreamSupport.stream(new OrdersSpliterator(bestAskOrder), false);
    }

    @Override
    public Stream<DirectOrder> bidOrdersStream(boolean sortedIgnore) {
        return StreamSupport.stream(new OrdersSpliterator(bestBidOrder), false);
    }

    @Override
    public void fillAsks(final int size, L2MarketData data) {
        data.askSize = 0;
        askPriceBuckets.forEach((p, bucket) -> {
            final int i = data.askSize++;
            data.askPrices[i] = bucket.tail.price;
            data.askVolumes[i] = bucket.volume;
            data.askOrders[i] = bucket.numOrders;
        }, size);
    }

    @Override
    public void fillBids(final int size, L2MarketData data) {
        data.bidSize = 0;
        bidPriceBuckets.forEachDesc((p, bucket) -> {
            final int i = data.bidSize++;
            data.bidPrices[i] = bucket.tail.price;
            data.bidVolumes[i] = bucket.volume;
            data.bidOrders[i] = bucket.numOrders;
        }, size);
    }

    /**
     * 获取总卖单桶
     *
     * @param limit 界限
     * @return
     */
    @Override
    public int getTotalAskBuckets(final int limit) {
        return askPriceBuckets.size(limit);
    }

    /**
     * 获取总买单桶
     *
     * @param limit 界限
     * @return
     */
    @Override
    public int getTotalBidBuckets(final int limit) {
        return bidPriceBuckets.size(limit);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeByte(getImplementationType().getCode());
        symbolSpec.writeMarshallable(bytes);
        bytes.writeInt(orderIdIndex.size(Integer.MAX_VALUE));
        askOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
        bidOrdersStream(true).forEach(order -> order.writeMarshallable(bytes));
    }

    /**
     * 新Gtc订单
     *
     * @param cmd
     */
    private void newOrderPlaceGtc(final OrderCommand cmd) {
        final long size = cmd.size;

        // check if order is marketable there are matching orders 尝试匹配订单
        final long filledSize = tryMatchInstantly(cmd, cmd);
        if (filledSize == size) {
            // completed before being placed - can just return 匹配完成直接返回
            return;
        }

        final long orderId = cmd.orderId;
        // TODO eliminate double hashtable lookup?
        if (orderIdIndex.get(orderId) != null) { // containsKey for hashtable
            // duplicate order id - can match, but can not place
            eventsHelper.attachRejectEvent(cmd, size - filledSize);
            log.warn("duplicate order id: {}", cmd);
            return;
        }

        final long price = cmd.price;

        // normally placing regular GTC order
        final DirectOrder orderRecord = objectsPool.get(ObjectsPool.DIRECT_ORDER, (Supplier<DirectOrder>) DirectOrder::new);

        orderRecord.orderId = orderId;
        orderRecord.price = price;
        orderRecord.size = size;
        orderRecord.reserveBidPrice = cmd.reserveBidPrice;
        orderRecord.action = cmd.action;
        orderRecord.uid = cmd.uid;
        orderRecord.timestamp = cmd.timestamp;
        orderRecord.filled = filledSize;

        orderIdIndex.put(orderId, orderRecord);
        insertOrder(orderRecord, null);
    }

    /**
     * 新Ioc订单
     *
     * @param cmd
     */
    private void newOrderMatchIoc(final OrderCommand cmd) {

        final long filledSize = tryMatchInstantly(cmd, cmd);

        final long rejectedSize = cmd.size - filledSize;

        if (rejectedSize != 0) {
            // was not matched completely - send reject for not-completed IoC order
            eventsHelper.attachRejectEvent(cmd, rejectedSize);
        }
    }

    /**
     * 新FOK Budget订单
     *
     * @param cmd
     */
    private void newOrderMatchFokBudget(final OrderCommand cmd) {

        final long budget = checkBudgetToFill(cmd.action, cmd.size);

        if (logDebug) log.debug("Budget calc: {} requested: {}", budget, cmd.price);

        if (isBudgetLimitSatisfied(cmd.action, budget, cmd.price)) {
            tryMatchInstantly(cmd, cmd);
        } else {
            eventsHelper.attachRejectEvent(cmd, cmd.size);
        }
    }

    private boolean isBudgetLimitSatisfied(final OrderAction orderAction, final long calculated, final long limit) {
        return calculated != Long.MAX_VALUE
                && (calculated == limit || (orderAction == OrderAction.BID ^ calculated > limit));
    }

    private long checkBudgetToFill(final OrderAction action,
                                   long size) {

        DirectOrder makerOrder = (action == OrderAction.BID) ? bestAskOrder : bestBidOrder;

        long budget = 0L;

        // iterate through all orders
        while (makerOrder != null) {
            final Bucket bucket = makerOrder.parent;

            final long availableSize = bucket.volume;
            final long price = makerOrder.price;

            if (size > availableSize) {
                size -= availableSize;
                budget += availableSize * price;
                if (logDebug) log.debug("add    {} * {} -> {}", price, availableSize, budget);
            } else {
                if (logDebug) log.debug("return {} * {} -> {}", price, size, budget + size * price);
                return budget + size * price;
            }

            // switch to next order (can be null)
            makerOrder = bucket.tail.prev;
        }
        if (logDebug) log.debug("not enough liquidity to fill size={}", size);
        return Long.MAX_VALUE;
    }

    /**
     * 尝试订单匹配：新订单进入时，会尝试与对手方的最优订单进行撮合（tryMatchInstantly方法）。撮合过程中，会更新订单状态，生成交易事件，并移除完全成交的订单。
     *
     * @param takerOrder 接收到的订单，可以是买入或卖出订单
     * @param triggerCmd 触发命令，包含订单类型和命令类型
     *
     * @return 已成交量
     */
    private long tryMatchInstantly(final IOrder takerOrder,
                                   final OrderCommand triggerCmd) {

        // 表示接收到的订单是否为买入订单
        final boolean isBidAction = takerOrder.getAction() == OrderAction.BID;
        // 根据订单类型和方向确定的限价。如果是卖出方向的FOK_BUDGET订单且不是买入动作，则限价为0；否则为订单报价
        final long limitPrice = (triggerCmd.command == OrderCommandType.PLACE_ORDER && triggerCmd.orderType == OrderType.FOK_BUDGET && !isBidAction)
                ? 0L
                : takerOrder.getPrice();

        // 选择市场订单
        DirectOrder makerOrder;
        if (isBidAction) { // 如果是买入订单（isBidAction为真），则选择最优卖单（bestAskOrder）
            makerOrder = bestAskOrder;
            if (makerOrder == null || makerOrder.price > limitPrice) { // 市场最优卖单价格高于买单限价,即无可匹配订单,返回已成交量
                return takerOrder.getFilled();
            }
        } else { // 如果是卖出订单，则选择最优买单（bestBidOrder）
            makerOrder = bestBidOrder;
            if (makerOrder == null || makerOrder.price < limitPrice) { // 市场最优买单价格低于卖单限价,即无可匹配订单,返回已成交量
                return takerOrder.getFilled();
            }
        }

        // 主动买卖单的剩余量 = 总量 - 已成交量
        long remainingSize = takerOrder.getSize() - takerOrder.getFilled();
        // 如果剩余量为0，则返回已成交量
        if (remainingSize == 0) {
            return takerOrder.getFilled();
        }
        // 实时获取最优订单的所属桶的最后一个订单
        DirectOrder priceBucketTail = makerOrder.parent.tail;

        final long takerReserveBidPrice = takerOrder.getReserveBidPrice();
//        final long takerOrderTimestamp = takerOrder.getTimestamp();

//        log.debug("MATCHING taker: {} remainingSize={}", takerOrder, remainingSize);

        // 匹配订单事件链
        MatcherTradeEvent eventsTail = null;

        // iterate through all orders 循环遍历市场订单
        do {

//            log.debug("  matching from maker order: {}", makerOrder);

            // 计算每次匹配的成交量（tradeSize），更新市场挂单和订单桶的成交量
            // calculate exact volume can fill for this order 计算此次订单可成交量：v = min(主动买卖单的剩余需成交量, 市场挂单剩余量)
            final long tradeSize = Math.min(remainingSize, makerOrder.size - makerOrder.filled);
//                log.debug("  tradeSize: {} MIN(remainingSize={}, makerOrder={})", tradeSize, remainingSize, makerOrder.size - makerOrder.filled);
            // 更新市场挂单的已成交量
            makerOrder.filled += tradeSize;
            // 更新市场订单桶的剩余未成交量
            makerOrder.parent.volume -= tradeSize;
            // 更新主动买卖单的剩余成交量
            remainingSize -= tradeSize;
            // remove from order book filled orders
            // 如果市场挂单完全成交，则从订单薄中移除该挂单，并更新订单桶的订单数和引用。
            final boolean makerCompleted = makerOrder.size == makerOrder.filled;
            if (makerCompleted) {
                // 市场挂单完全成交,订单桶订单数-1
                makerOrder.parent.numOrders--;
            }
            // 生成订单匹配事件，并添加到事件链中
            final MatcherTradeEvent tradeEvent = eventsHelper.sendTradeEvent(makerOrder, makerCompleted, remainingSize == 0, tradeSize,
                    isBidAction ? takerReserveBidPrice : makerOrder.reserveBidPrice);
            if (eventsTail == null) {
                // 链尾为空,链表还未开始构建,当前事件节点为链头
                triggerCmd.matcherEvent = tradeEvent;
            } else {
                // 链接新事件
                eventsTail.nextEvent = tradeEvent;
            }
            // 更新链尾指针
            eventsTail = tradeEvent;

            if (!makerCompleted) {
                // 市场挂单未完全成交
                // maker not completed -> no unmatched volume left, can exit matching loop
//                    log.debug("  not completed, exit");
                break;
            }

            // if completed can remove maker order 如果已完全成交
            orderIdIndex.remove(makerOrder.orderId);
            objectsPool.put(ObjectsPool.DIRECT_ORDER, makerOrder);


            if (makerOrder == priceBucketTail) {
                // reached current price tail -> remove bucket reference 达到当前价格尾部 -> 移除桶引用
                final LongAdaptiveRadixTreeMap<Bucket> buckets = isBidAction ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(makerOrder.price);
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, makerOrder.parent);
//                log.debug("  removed price bucket for {}", makerOrder.price);

                // set next price tail (if there is next price)
                if (makerOrder.prev != null) {
                    priceBucketTail = makerOrder.prev.parent.tail;
                }
            }

            // switch to next order 转到下一个市场订单
            makerOrder = makerOrder.prev; // can be null 可能为空

        } while (makerOrder != null
                && remainingSize > 0
                && (isBidAction ? makerOrder.price <= limitPrice : makerOrder.price >= limitPrice));

        // break chain after last order
        if (makerOrder != null) {
            makerOrder.next = null;
        }

//        log.debug("makerOrder = {}", makerOrder);
//        log.debug("makerOrder.parent = {}", makerOrder != null ? makerOrder.parent : null);

        // update best orders reference
        if (isBidAction) {
            bestAskOrder = makerOrder;
        } else {
            bestBidOrder = makerOrder;
        }

        // return filled amount
        return takerOrder.getSize() - remainingSize;
    }

    /**
     * 从桶中删除订单
     *
     *
     * @param order
     *
     * @return 操作的桶
     */
    private Bucket removeOrder(final DirectOrder order) {

        final Bucket bucket = order.parent;
        bucket.volume -= order.size - order.filled;
        bucket.numOrders--;
        Bucket bucketRemoved = null;

        if (bucket.tail == order) {
            // if we removing tail order -> change bucket tail reference
            if (order.next == null || order.next.parent != bucket) {
                // if no next or next order has different parent -> then it was the last bucket -> remove record
                final LongAdaptiveRadixTreeMap<Bucket> buckets = order.action == OrderAction.ASK ? askPriceBuckets : bidPriceBuckets;
                buckets.remove(order.price);
                bucketRemoved = bucket;
            } else {
                // otherwise at least one order always having the same parent left -> update tail reference to it
                bucket.tail = order.next; // always not null
            }
        }

        // update neighbor orders
        if (order.next != null) {
            order.next.prev = order.prev; // can be null
        }
        if (order.prev != null) {
            order.prev.next = order.next; // can be null
        }

        // check if best ask/bid were referring to the order we just removed
        if (order == bestAskOrder) {
            bestAskOrder = order.prev; // can be null
        } else if (order == bestBidOrder) {
            bestBidOrder = order.prev; // can be null
        }

        return bucketRemoved;
    }

    /**
     * 插入订单
     *
     * @param order 要插入的新订单
     * @param freeBucket 一个可重用的空订单桶
     */
    private void insertOrder(final DirectOrder order, final Bucket freeBucket) {

//        log.debug("   + insert order: {}", order);
        // 一个布尔值，表示订单是否为卖单（ASK）
        final boolean isAsk = order.action == OrderAction.ASK;
        // 根据isAsk选择对应的订单桶地图
        final LongAdaptiveRadixTreeMap<Bucket> buckets = isAsk ? askPriceBuckets : bidPriceBuckets;
        // 从订单桶地图中获取与订单价格对应的桶
        final Bucket toBucket = buckets.get(order.price);
        // 如果toBucket不为null，说明订单价格对应的桶已经存在
        if (toBucket != null) {
            // update tail if bucket already exists6
//            log.debug(">>>> increment bucket {} from {} to {}", toBucket.tail.price, toBucket.volume, toBucket.volume +  order.size - order.filled);

            // can put bucket back to the pool (because target bucket already exists)
            if (freeBucket != null) {
                objectsPool.put(ObjectsPool.DIRECT_BUCKET, freeBucket);
            }

            toBucket.volume += order.size - order.filled;
            toBucket.numOrders++;
            final DirectOrder oldTail = toBucket.tail; // always exists, not null
            final DirectOrder prevOrder = oldTail.prev; // can be null
            // update neighbors
            toBucket.tail = order;
            oldTail.prev = order;
            if (prevOrder != null) {
                prevOrder.next = order;
            }
            // update self
            order.next = oldTail;
            order.prev = prevOrder;
            order.parent = toBucket;

        } else {

            // insert a new bucket (reuse existing)
            final Bucket newBucket = freeBucket != null
                    ? freeBucket
                    : objectsPool.get(ObjectsPool.DIRECT_BUCKET, Bucket::new);

            newBucket.tail = order;
            newBucket.volume = order.size - order.filled;
            newBucket.numOrders = 1;
            order.parent = newBucket;
            buckets.put(order.price, newBucket);
            final Bucket lowerBucket = isAsk ? buckets.getLowerValue(order.price) : buckets.getHigherValue(order.price);
            if (lowerBucket != null) {
                // attache new bucket and event to the lower entry
                DirectOrder lowerTail = lowerBucket.tail;
                final DirectOrder prevOrder = lowerTail.prev; // can be null
                // update neighbors
                lowerTail.prev = order;
                if (prevOrder != null) {
                    prevOrder.next = order;
                }
                // update self
                order.next = lowerTail;
                order.prev = prevOrder;
            } else {

                // if no floor entry, then update best order
                final DirectOrder oldBestOrder = isAsk ? bestAskOrder : bestBidOrder; // can be null

                if (oldBestOrder != null) {
                    oldBestOrder.next = order;
                }

                if (isAsk) {
                    bestAskOrder = order;
                } else {
                    bestBidOrder = order;
                }

                // update self
                order.next = null;
                order.prev = oldBestOrder;
            }
        }
    }

    private void validateChain(boolean asksChain, Long2ObjectHashMap<DirectOrder> ordersInChain) {

        // buckets index
        final LongAdaptiveRadixTreeMap<Bucket> buckets = asksChain ? askPriceBuckets : bidPriceBuckets;
        final LongObjectHashMap<Bucket> bucketsFoundInChain = new LongObjectHashMap<>();
        buckets.validateInternalState();

        DirectOrder order = asksChain ? bestAskOrder : bestBidOrder;

        if (order != null && order.next != null) {
            thrw("best order has not-null next reference");
        }

//        log.debug("----------- validating {} --------- ", asksChain ? OrderAction.ASK : OrderAction.BID);

        long lastPrice = -1;
        long expectedBucketVolume = 0;
        int expectedBucketOrders = 0;
        DirectOrder lastOrder = null;

        while (order != null) {

            if (ordersInChain.containsKey(order.orderId)) {
                thrw("duplicate orderid in the chain");
            }
            ordersInChain.put(order.orderId, order);

            //log.debug("id:{} p={} +{}", order.orderId, order.price, order.size - order.filled);
            expectedBucketVolume += order.size - order.filled;
            expectedBucketOrders++;

            if (lastOrder != null && order.next != lastOrder) {
                thrw("incorrect next reference");
            }
            if (order.parent.tail.price != order.price) {
                thrw("price of parent.tail differs");
            }
            if (lastPrice != -1 && order.price != lastPrice) {
                if (asksChain ^ order.price > lastPrice) {
                    thrw("unexpected price change direction");
                }
                if (order.next.parent == order.parent) {
                    thrw("unexpected price change within same bucket");
                }
            }

            if (order.parent.tail == order) {
                if (order.parent.volume != expectedBucketVolume) {
                    thrw("bucket volume does not match orders chain sizes");
                }
                if (order.parent.numOrders != expectedBucketOrders) {
                    thrw("bucket numOrders does not match orders chain length");
                }
                if (order.prev != null && order.prev.price == order.price) {
                    thrw("previous bucket has the same price");
                }
                expectedBucketVolume = 0;
                expectedBucketOrders = 0;
            }

            final Bucket knownBucket = bucketsFoundInChain.get(order.price);
            if (knownBucket == null) {
                bucketsFoundInChain.put(order.price, order.parent);
            } else if (knownBucket != order.parent) {
                thrw("found two different buckets having same price");
            }

            if (asksChain ^ order.action == OrderAction.ASK) {
                thrw("not expected order action");
            }

            lastPrice = order.price;
            lastOrder = order;
            order = order.prev;
        }

        // validate last order
        if (lastOrder != null && lastOrder.parent.tail != lastOrder) {
            thrw("last order is not a tail");
        }

//        log.debug("-------- validateChain ----- asksChain={} ", asksChain);
        buckets.forEach((price, bucket) -> {
//            log.debug("Remove {} ", price);
            if (bucketsFoundInChain.remove(price) != bucket) thrw("bucket in the price-tree not found in the chain");
        }, Integer.MAX_VALUE);

        if (!bucketsFoundInChain.isEmpty()) {
            thrw("found buckets in the chain that not discoverable from the price-tree");
        }
    }

//    private void dumpNearOrders(final DirectOrder order, int maxNeighbors) {
//        if (order == null) {
//            log.debug("no orders");
//            return;
//        }
//        DirectOrder p = order;
//        for (int i = 0; i < maxNeighbors && p.prev != null; i++) {
//            p = p.prev;
//        }
//        for (int i = 0; i < maxNeighbors * 2 && p != null; i++) {
//            log.debug(((p == order) ? "*" : " ") + "  {}\t -> \t{}", p, p.parent);
//            p = p.next;
//        }
//    }

    private void thrw(final String msg) {
        throw new IllegalStateException(msg);
    }


    /**
     * 订单
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static final class DirectOrder implements WriteBytesMarshallable, IOrder {

        /**
         * 订单ID
         */
        @Getter
        public long orderId;

        /**
         * 订单价格
         */
        @Getter
        public long price;

        /**
         * 订单数量
         */
        @Getter
        public long size;

        /**
         * 已成交数量
         */
        @Getter
        public long filled;

        // new orders - reserved price for fast moves of GTC bid orders in exchange mode
        /**
         * 保留的投标价格，用于GTC（Good Till Cancelled，即撤销前有效）订单在交易所模式下的快速移动，是一个长整型变量
         */
        @Getter
        public long reserveBidPrice;

        // required for PLACE_ORDER only;
        // 订单行为类型枚举，仅用于下单，是一个枚举类型变量
        @Getter
        public OrderAction action;

        /**
         * 用户ID
         */
        @Getter
        public long uid;

        /**
         * 时间戳
         */
        @Getter
        public long timestamp;

        // fast orders structure
        /**
         * 快速订单结构
         * 指向所属订单桶
         */
        Bucket parent;

        // next order (towards the matching direction, price grows for asks)
        // 下一个订单（朝着匹配的方向，对于卖单价格递增）
        DirectOrder next;

        // previous order (to the tail of the queue, lower priority and worst price, towards the matching direction)
        // 上一个订单（位于队列尾部，优先级较低且价格最差，朝向匹配方向）
        DirectOrder prev;


        // public int userCookie;

        /**
         * 用于从字节数组中读取并初始化订单的相关信息。构造函数中通过bytes.readLong()和bytes.readByte()等方法从BytesIn对象中读取数据，并分别赋值给对应的变量
         *
         * @param bytes 字节输入
         */
        public DirectOrder(BytesIn bytes) {


            this.orderId = bytes.readLong(); // orderId
            this.price = bytes.readLong();  // price
            this.size = bytes.readLong(); // size
            this.filled = bytes.readLong(); // filled
            this.reserveBidPrice = bytes.readLong(); // price2
            this.action = OrderAction.of(bytes.readByte());
            this.uid = bytes.readLong(); // uid
            this.timestamp = bytes.readLong(); // timestamp
            // this.userCookie = bytes.readInt();  // userCookie

            // TODO
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeLong(orderId);
            bytes.writeLong(price);
            bytes.writeLong(size);
            bytes.writeLong(filled);
            bytes.writeLong(reserveBidPrice);
            bytes.writeByte(action.getCode());
            bytes.writeLong(uid);
            bytes.writeLong(timestamp);
            // bytes.writeInt(userCookie);
            // TODO
        }

        @Override
        public String toString() {
            return "[" + orderId + " " + (action == OrderAction.ASK ? 'A' : 'B')
                    + price + ":" + size + "F" + filled
                    // + " C" + userCookie
                    + " U" + uid + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                    //userCookie,
                    uid);
        }


        /**
         * timestamp is not included into hashCode() and equals() for repeatable results
         */
        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            if (!(o instanceof DirectOrder)) return false;

            DirectOrder other = (DirectOrder) o;

            // ignore userCookie && timestamp
            return orderId == other.orderId
                    && action == other.action
                    && price == other.price
                    && size == other.size
                    && reserveBidPrice == other.reserveBidPrice
                    && filled == other.filled
                    && uid == other.uid;
        }

        @Override
        public int stateHash() {
            return Objects.hash(orderId, action, price, size, reserveBidPrice, filled,
                    //userCookie,
                    uid);
        }
    }

    /**
     * 订单桶
     */
    @ToString
    private static class Bucket {
        /**
         * 量
         */
        long volume;
        /**
         * 订单数
         */
        int numOrders;
        /**
         * 链尾订单
         */
        DirectOrder tail;
    }
}
