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

import lombok.AllArgsConstructor;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * 订单分段器
 */
@AllArgsConstructor
public final class OrdersSpliterator implements Spliterator<OrderBookDirectImpl.DirectOrder> {

    /**
     * 用于跟踪订单序列中的当前位置的指针
     */
    private OrderBookDirectImpl.DirectOrder pointer;

    /**
     * 接受一个类型为Consumer的参数，这个消费者可以处理OrderBookDirectImpl.DirectOrder类型或者其超类型的数据
     *
     * @param action 消费者函数
     *
     * @return boolean
     */
    @Override
    public boolean tryAdvance(Consumer<? super OrderBookDirectImpl.DirectOrder> action) {
        if (pointer == null) {
            return false;
        } else {
            action.accept(pointer);
            // 将pointer移动到前一个订单
            pointer = pointer.prev;
            return true;
        }
    }

    @Override
    public Spliterator<OrderBookDirectImpl.DirectOrder> trySplit() {
        // 目前的实现总是返回null，这意味着这个分割器不支持分割操作
        return null;
    }

    @Override
    public long estimateSize() {
        // 它返回Long.MAX_VALUE，表示无法准确估计分割器中元素的数量
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        // 它返回Spliterator.ORDERED，表示这个分割器处理的数据是有序的
        return Spliterator.ORDERED;
    }
}
